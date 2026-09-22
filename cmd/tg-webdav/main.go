package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"encoding/json"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/session"
	"github.com/joho/godotenv"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/message/unpack"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"
	"golang.org/x/net/webdav"
	_ "modernc.org/sqlite"
)

type Config struct {
	Addr, BotToken, APIHash, User, Pass, Folder, DB, Session string
	APIID int
	Channel int64
	Chunk int
	CacheMB int64
}

func env(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" { return v }
	return d
}

func loadConfig() Config {
	id, _ := strconv.Atoi(env("TELEGRAM_API_ID", "0"))
	ch, _ := strconv.ParseInt(env("TELEGRAM_CHANNEL_ID", "0"), 10, 64)
	chunk, _ := strconv.Atoi(env("CHUNK_SIZE", "524288"))
	mb, _ := strconv.ParseInt(env("CACHE_MB", "256"), 10, 64)
	return Config{
		Addr: env("LISTEN_ADDR", ":"+env("PORT", "8080")),
		BotToken: env("TELEGRAM_BOT_TOKEN", ""),
		APIHash: env("TELEGRAM_API_HASH", ""),
		User: env("WEBDAV_USERNAME", "admin"),
		Pass: env("WEBDAV_PASSWORD", "change-me"),
		Folder: strings.Trim(path.Clean("/"+env("DEFAULT_FOLDER", "general")), "/"),
		DB: env("DB_PATH", "./data/tg-webdav.db"),
		Session: env("SESSION_PATH", "./data/telegram.session"),
		APIID: id, Channel: ch, Chunk: chunk, CacheMB: mb,
	}
}

type Item struct {
	Path, Name, MIME string
	Size int64
	Dir bool
	Mod time.Time
	Loc []byte
}

type Store struct{ db *sql.DB }

func openStore(c Config) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(c.DB), 0700); err != nil { return nil, err }
	db, err := sql.Open("sqlite", c.DB)
	if err != nil { return nil, err }
	s := &Store{db}
	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS items(path TEXT PRIMARY KEY,name TEXT,is_dir INTEGER,size INTEGER,mime TEXT,mod_time INTEGER,location BLOB); CREATE INDEX IF NOT EXISTS idx_items_name ON items(name);"); err != nil { return nil, err }
	return s, s.ensureFolder("/"+c.Folder)
}

func clean(p string) string {
	p = path.Clean("/" + strings.ReplaceAll(p, "\\", "/"))
	if p == "." { return "/" }
	return p
}

func (s *Store) ensureFolder(p string) error {
	p = clean(p)
	cur := ""
	for _, part := range strings.Split(strings.Trim(p, "/"), "/") {
		if part == "" { continue }
		cur += "/" + part
		if _, err := s.db.Exec("INSERT OR IGNORE INTO items(path,name,is_dir,size,mime,mod_time) VALUES(?,?,1,0,'inode/directory',?)", cur, part, time.Now().Unix()); err != nil { return err }
	}
	return nil
}

func (s *Store) stat(p string) (Item, error) {
	p = clean(p)
	if p == "/" { return Item{Path: "/", Dir: true}, nil }
	var i Item
	var d, mt int64
	err := s.db.QueryRow("SELECT path,name,is_dir,size,mime,mod_time,location FROM items WHERE path=?", p).Scan(&i.Path,&i.Name,&d,&i.Size,&i.MIME,&mt,&i.Loc)
	if errors.Is(err, sql.ErrNoRows) { return i, os.ErrNotExist }
	if err != nil { return i, err }
	i.Dir, i.Mod = d != 0, time.Unix(mt,0)
	return i, nil
}

func (s *Store) list(p string) ([]Item, error) {
	p = clean(p)
	rows, err := s.db.Query("SELECT path,name,is_dir,size,mime,mod_time,location FROM items WHERE path LIKE ? ORDER BY is_dir DESC,name COLLATE NOCASE", strings.TrimRight(p,"/")+"/%")
	if err != nil { return nil, err }
	defer rows.Close()
	var out []Item
	for rows.Next() {
		var i Item
		var d, mt int64
		if err := rows.Scan(&i.Path,&i.Name,&d,&i.Size,&i.MIME,&mt,&i.Loc); err != nil { return nil, err }
		if path.Dir(i.Path) != strings.TrimRight(p,"/") { continue }
		i.Dir, i.Mod = d != 0, time.Unix(mt,0)
		out = append(out,i)
	}
	return out, rows.Err()
}

type locationBox struct {
	Doc *tg.InputDocumentFileLocation
	Photo *tg.InputPhotoFileLocation
}

func encodeLocation(loc tg.InputFileLocationClass) ([]byte,error) {
	var b bytes.Buffer
	x := locationBox{}
	switch v := loc.(type) {
	case *tg.InputDocumentFileLocation: x.Doc = v
	case *tg.InputPhotoFileLocation: x.Photo = v
	default: return nil, fmt.Errorf("unsupported Telegram location %T",loc)
	}
	if err := gob.NewEncoder(&b).Encode(x); err != nil { return nil,err }
	return b.Bytes(),nil
}

func decodeLocation(b []byte) (tg.InputFileLocationClass,error) {
	var x locationBox
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&x); err != nil { return nil,err }
	if x.Doc != nil { return x.Doc,nil }
	if x.Photo != nil { return x.Photo,nil }
	return nil,errors.New("invalid Telegram location")
}

func init() {
	gob.Register(&tg.InputDocumentFileLocation{})
	gob.Register(&tg.InputPhotoFileLocation{})
}

func (s *Store) remove(p string) error {
 p=clean(p); if p=="/" { return errors.New("cannot remove root") }
 _,err:=s.db.Exec("DELETE FROM items WHERE path=? OR path LIKE ?",p,strings.TrimRight(p,"/")+"/%"); return err
}
func (s *Store) rename(oldP,newP string) error {
 oldP,newP=clean(oldP),clean(newP)
 if oldP=="/" || newP=="/" { return errors.New("invalid rename") }
 if _,e:=s.stat(oldP);e!=nil{return e}
 if _,e:=s.stat(newP);e==nil{return os.ErrExist}
 if e:=s.ensureFolder(path.Dir(newP));e!=nil{return e}
 rows,e:=s.db.Query("SELECT path,name,is_dir,size,mime,mod_time,location FROM items WHERE path=? OR path LIKE ? ORDER BY length(path)",oldP,strings.TrimRight(oldP,"/")+"/%");if e!=nil{return e};defer rows.Close()
 type pair struct{i Item}
 var all []pair
 for rows.Next(){var x Item;var d,mt int64;if e:=rows.Scan(&x.Path,&x.Name,&d,&x.Size,&x.MIME,&mt,&x.Loc);e!=nil{return e};x.Dir=d!=0;x.Mod=time.Unix(mt,0);all=append(all,pair{x})}
 for _,p:=range all{np:=newP+strings.TrimPrefix(p.i.Path,oldP);_,e:=s.db.Exec("INSERT OR REPLACE INTO items(path,name,is_dir,size,mime,mod_time,location) VALUES(?,?,?,?,?,?,?)",np,path.Base(np),boolInt(p.i.Dir),p.i.Size,p.i.MIME,p.i.Mod.Unix(),p.i.Loc);if e!=nil{return e}}
 return s.remove(oldP)
}
func boolInt(v bool) int {if v{return 1};return 0}

func (s *Store) upsert(i Item) error {
 _,err:=s.db.Exec("INSERT OR REPLACE INTO items(path,name,is_dir,size,mime,mod_time,location) VALUES(?,?,?,?,?,?,?)",i.Path,i.Name,boolInt(i.Dir),i.Size,i.MIME,i.Mod.Unix(),i.Loc)
 return err
}

type Telegram struct {
	cfg Config
	store *Store
	client *telegram.Client
	peers *peers.Manager
}

func (t *Telegram) run(ctx context.Context) error {
	if t.cfg.APIID == 0 || t.cfg.APIHash == "" || t.cfg.BotToken == "" {
		return errors.New("set TELEGRAM_API_ID, TELEGRAM_API_HASH and TELEGRAM_BOT_TOKEN")
	}
	if err := os.MkdirAll(filepath.Dir(t.cfg.Session),0700); err != nil { return err }
	d := tg.NewUpdateDispatcher()
	t.client = telegram.NewClient(t.cfg.APIID,t.cfg.APIHash,telegram.Options{
		SessionStorage:&session.FileStorage{Path:t.cfg.Session},
		UpdateHandler:d, AllowCDN:true,
	})
	d.OnNewMessage(func(_ context.Context,_ tg.Entities,u *tg.UpdateNewMessage) error {
		m,ok:=u.Message.(*tg.Message); if !ok || m.Out { return nil }
		return t.ingest(m)
	})
	return t.client.Run(ctx,func(ctx context.Context) error {
		if _,err:=t.client.Auth().Bot(ctx,t.cfg.BotToken); err!=nil { return err }
		t.peers = peers.Options{}.Build(t.client.API())
		if err:=t.peers.Init(ctx); err!=nil { return err }
		return telegram.RunUntilCanceled(ctx,t.client)
	})
}

func (t *Telegram) forward(ctx context.Context, fromChat int64, msgID int64) error {
	if t.cfg.Channel == 0 || fromChat == 0 { return nil }
	v := url.Values{}
	v.Set("chat_id", strconv.FormatInt(t.cfg.Channel, 10))
	v.Set("from_chat_id", strconv.FormatInt(fromChat, 10))
	v.Set("message_id", strconv.FormatInt(msgID, 10))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.telegram.org/bot"+t.cfg.BotToken+"/forwardMessage", strings.NewReader(v.Encode()))
	if err != nil { return err }
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 { b,_:=io.ReadAll(io.LimitReader(resp.Body,4096)); return fmt.Errorf("forwardMessage: %s: %s",resp.Status,strings.TrimSpace(string(b))) }
	return nil
}

func peerID(p tg.PeerClass) int64 {
	switch x := p.(type) {
	case *tg.PeerUser: return x.UserID
	case *tg.PeerChat: return int64(x.ChatID)
	case *tg.PeerChannel: return int64(x.ChannelID)
	default: return 0
	}
}

func (t *Telegram) ingest(m *tg.Message) error {
	var loc tg.InputFileLocationClass
	var size int64
	name := ""
	mt := "application/octet-stream"
	switch x := m.Media.(type) {
	case *tg.MessageMediaDocument:
		d,ok:=x.Document.(*tg.Document); if !ok { return nil }
		loc=&tg.InputDocumentFileLocation{ID:d.ID,AccessHash:d.AccessHash,FileReference:d.FileReference}
		size=d.Size; mt=d.MimeType
		for _,a:=range d.Attributes { if f,ok:=a.(*tg.DocumentAttributeFilename);ok { name=f.FileName } }
	case *tg.MessageMediaPhoto:
		p,ok:=x.Photo.(*tg.Photo); if !ok { return nil }
		loc=&tg.InputPhotoFileLocation{ID:p.ID,AccessHash:p.AccessHash,FileReference:p.FileReference}
		name="image_"+strconv.FormatInt(int64(m.ID),10)+".jpg"; mt="image/jpeg"
	default: return nil
	}
	if strings.TrimSpace(m.Message)!="" { name=strings.TrimSpace(m.Message) }
	name=strings.NewReplacer("/","_","\\","_").Replace(name)
	if filepath.Ext(name)=="" { if ex,_:=mime.ExtensionsByType(mt);len(ex)>0{name+=ex[0]} }
	if name=="" { name="file_"+strconv.FormatInt(int64(m.ID),10) }
	p:=clean("/"+t.cfg.Folder+"/"+name)
	if err := t.forward(context.Background(), peerID(m.PeerID), int64(m.ID)); err != nil { log.Printf("forward: %v", err) }
	b,err:=encodeLocation(loc);if err!=nil{return err}
	_,err=t.store.db.Exec("INSERT OR REPLACE INTO items(path,name,is_dir,size,mime,mod_time,location) VALUES(?,?,0,?,?,?,?)",p,name,size,mt,time.Unix(int64(m.Date),0),b)
	return err
}

type cache struct{ mu sync.Mutex; m map[string][]byte; order []string; size,max int64 }

func newCache(mb int64)*cache{return &cache{m:map[string][]byte{},max:mb*1024*1024}}
func(c *cache)get(k string)([]byte,bool){c.mu.Lock();defer c.mu.Unlock();b,ok:=c.m[k];if !ok{return nil,false};return append([]byte(nil),b...),true}
func(c *cache)set(k string,b []byte){c.mu.Lock();defer c.mu.Unlock();c.m[k]=append([]byte(nil),b...);c.size+=int64(len(b));c.order=append(c.order,k);for c.size>c.max&&len(c.order)>0{x:=c.order[0];c.order=c.order[1:];if old,ok:=c.m[x];ok{delete(c.m,x);c.size-=int64(len(old))}}}

type fileInfo struct{ name string; size int64; dir bool; mod time.Time }
func(i fileInfo)Name()string{return i.name};func(i fileInfo)Size()int64{return i.size};func(i fileInfo)Mode()os.FileMode{if i.dir{return os.ModeDir|0755};return 0444};func(i fileInfo)ModTime()time.Time{return i.mod};func(i fileInfo)IsDir()bool{return i.dir};func(i fileInfo)Sys()any{return nil}

type rangeFile struct{ctx context.Context; client *telegram.Client; loc tg.InputFileLocationClass; size,off int64; name string; chunk int; cache *cache}
func(f *rangeFile)Stat()(os.FileInfo,error){return fileInfo{f.name,f.size,false,time.Now()},nil}
func(f *rangeFile)Close()error{return nil};func(f *rangeFile)Readdir(int)([]os.FileInfo,error){return nil,nil};func(f *rangeFile)Write([]byte)(int,error){return 0,errors.New("read-only file")}
func(f *rangeFile)Seek(o int64,w int)(int64,error){switch w{case io.SeekStart:f.off=o;case io.SeekCurrent:f.off+=o;case io.SeekEnd:f.off=f.size+o;default:return f.off,errors.New("bad seek")};if f.off<0{return f.off,errors.New("negative seek")};return f.off,nil}
func(f *rangeFile)Read(p []byte)(int,error){if f.off>=f.size{return 0,io.EOF};n:=int64(len(p));if n>f.size-f.off{n=f.size-f.off};got,e:=f.readAt(p[:n],f.off);f.off+=int64(got);return got,e}
func(f *rangeFile)readAt(p []byte,off int64)(int,error){total:=0;for total<len(p){idx:=off/int64(f.chunk);in:=off%int64(f.chunk);b,e:=f.chunkData(idx);if e!=nil{return total,e};if in>=int64(len(b)){return total,io.EOF};n:=copy(p[total:],b[in:]);total+=n;off+=int64(n)};return total,nil}
func(f *rangeFile)chunkData(idx int64)([]byte,error){k:=fmt.Sprintf("%s:%d",f.name,idx);if b,ok:=f.cache.get(k);ok{return b,nil};off:=idx*int64(f.chunk);lim:=f.chunk;if r:=f.size-off;r<int64(lim){lim=int(r)};res,e:=f.client.API().UploadGetFile(f.ctx,&tg.UploadGetFileRequest{Location:f.loc,Offset:off,Limit:lim,Precise:true});if e!=nil{return nil,e};x,ok:=res.(*tg.UploadFile);if !ok{return nil,fmt.Errorf("Telegram returned %T",res)};f.cache.set(k,x.Bytes);return x.Bytes,nil}

type dirFile struct{inf fileInfo; items []Item; pos int}
func(d *dirFile)Close()error{return nil};func(d *dirFile)Read([]byte)(int,error){return 0,io.EOF};func(d *dirFile)Write([]byte)(int,error){return 0,errors.New("read-only directory")};func(d *dirFile)Seek(int64,int)(int64,error){return 0,errors.New("directory seek unsupported")};func(d *dirFile)Stat()(os.FileInfo,error){return d.inf,nil}
func(d *dirFile)Readdir(n int)([]os.FileInfo,error){if d.pos>=len(d.items){return nil,io.EOF};end:=len(d.items);if n>0&&d.pos+n<end{end=d.pos+n};out:=make([]os.FileInfo,0,end-d.pos);for _,x:=range d.items[d.pos:end]{out=append(out,fileInfo{x.Name,x.Size,x.Dir,x.Mod})};d.pos=end;return out,nil}

type FS struct{s *Store;t *Telegram;c *cache}
func(f *FS)Mkdir(_ context.Context,n string,_ os.FileMode)error{return f.s.ensureFolder(n)}
func(f *FS)Stat(_ context.Context,n string)(os.FileInfo,error){i,e:=f.s.stat(n);if e!=nil{return nil,e};return fileInfo{i.Name,i.Size,i.Dir,i.Mod},nil}
func(f *FS)RemoveAll(_ context.Context,n string)error{return f.s.remove(n)}
func(f *FS)Rename(_ context.Context,a,b string)error{return f.s.rename(a,b)}
type uploadFile struct{fs *FS; name string; file *os.File}
func (u *uploadFile) Close() error {
 if u.file==nil{return nil}
 if err:=u.file.Close();err!=nil{return err}
 defer os.Remove(u.file.Name())
 return u.fs.t.uploadFile(context.Background(),u.file.Name(),u.name)
}
func(u *uploadFile)Read(p []byte)(int,error){return u.file.Read(p)}
func(u *uploadFile)Write(p []byte)(int,error){return u.file.Write(p)}
func(u *uploadFile)Seek(o int64,w int)(int64,error){return u.file.Seek(o,w)}
func(u *uploadFile)Stat()(os.FileInfo,error){return u.file.Stat()}
func(u *uploadFile)Readdir(int)([]os.FileInfo,error){return nil,errors.New("not a directory")}

func(t *Telegram) uploadFile(ctx context.Context, local, name string) error {
 if t.client==nil || t.peers==nil{return errors.New("Telegram not connected")}
 ch,err:=t.peers.ResolveChannelID(ctx,t.cfg.Channel);if err!=nil{return err}
	s:=message.NewSender(t.client.API()).To(ch.InputPeer())
	upd,err:=s.Upload(message.FromPath(local)).File(ctx)
	m,err:=unpack.Message(upd,err);if err!=nil{return err}
	var loc tg.InputFileLocationClass;var size int64;mt:=mime.TypeByExtension(filepath.Ext(name))
	switch x:=m.Media.(type){
	case *tg.MessageMediaDocument:
		d,ok:=x.Document.(*tg.Document);if !ok{return errors.New("uploaded document missing")}
		loc=&tg.InputDocumentFileLocation{ID:d.ID,AccessHash:d.AccessHash,FileReference:d.FileReference};size=d.Size;if d.MimeType!=""{mt=d.MimeType}
	case *tg.MessageMediaPhoto:
		p,ok:=x.Photo.(*tg.Photo);if !ok{return errors.New("uploaded photo missing")}
		loc=&tg.InputPhotoFileLocation{ID:p.ID,AccessHash:p.AccessHash,FileReference:p.FileReference};size=0;if mt==""{mt="image/jpeg"}
	default:return errors.New("Telegram returned unsupported media")
	}
	b,err:=encodeLocation(loc);if err!=nil{return err}
	p:=clean("/"+t.cfg.Folder+"/"+name)
	return t.store.upsert(Item{Path:p,Name:name,Size:size,MIME:mt,Dir:false,Mod:time.Now(),Loc:b})
}

func(f *FS)OpenFile(ctx context.Context,n string,flag int,_ os.FileMode)(webdav.File,error){
 i,e:=f.s.stat(n)
 if e!=nil && (flag&(os.O_CREATE|os.O_WRONLY|os.O_RDWR))!=0 {
  if err:=f.s.ensureFolder(path.Dir(clean(n)));err!=nil{return nil,err}
  if err:=os.MkdirAll("./data/uploads",0700);err!=nil{return nil,err}
  tf,err:=os.CreateTemp("./data/uploads","put-*");if err!=nil{return nil,err}
  return &uploadFile{fs:f,name:path.Base(clean(n)),file:tf},nil
 }
 if e!=nil{return nil,e}
 if flag&(os.O_WRONLY|os.O_RDWR|os.O_TRUNC)!=0 && !i.Dir {
  if err:=os.MkdirAll("./data/uploads",0700);err!=nil{return nil,err}
  tf,err:=os.CreateTemp("./data/uploads","put-*");if err!=nil{return nil,err}
  return &uploadFile{fs:f,name:path.Base(clean(n)),file:tf},nil
 }
 if i.Dir{xs,e:=f.s.list(n);if e!=nil{return nil,e};return &dirFile{fileInfo{i.Name,0,true,i.Mod},xs,0},nil}
 if f.t.client==nil{return nil,errors.New("Telegram not connected")}
 l,e:=decodeLocation(i.Loc);if e!=nil{return nil,e}
 return &rangeFile{ctx,f.t.client,l,i.Size,0,i.Name,f.t.cfg.Chunk,f.c},nil
}

func basic(user, pass string, h http.Handler) http.Handler {
 return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
  u,p,ok:=r.BasicAuth()
  if !ok || u!=user || p!=pass {
   w.Header().Set("WWW-Authenticate", `Basic realm="Tg-webdav"`)
   http.Error(w,"unauthorized",http.StatusUnauthorized); return
  }
  h.ServeHTTP(w,r)
 })
}

const page = `<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>Tg-webdav</title><style>body{font-family:system-ui;background:#080d18;color:#eef;padding:20px;max-width:1100px;margin:auto}.bar{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}button,input{border:1px solid #30405f;background:#10192a;color:#fff;border-radius:9px;padding:9px 11px}input{flex:1}.i{display:flex;gap:10px;align-items:center;padding:12px;margin:7px 0;background:#101827;border:1px solid #1f2d46;border-radius:10px}.name{flex:1}.meta{color:#8492aa;font-size:12px}.danger{color:#ff8888}</style></head><body><h1>Telegram WebDAV</h1><div class=bar><button onclick="up()">↑ Up</button><button onclick="mkdir()">＋ Folder</button><button onclick="refresh()">↻ Refresh</button><input id=q placeholder="Search..." oninput=render()></div><div id=p></div><div id=l></div><script>let cur="/general",items=[];function esc(s){return s.replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll(""","&quot;")}async function api(u,o={}){let r=await fetch(u,o);if(r.status==401){alert("WebDAV credentials required.");throw Error("unauthorized")}if(!r.ok)throw Error(await r.text());return r}async function refresh(){let r=await api("/api/list?path="+encodeURIComponent(cur));items=(await r.json()).items||[];render()}function render(){let q=document.getElementById("q").value.toLowerCase();document.getElementById("p").textContent=cur;let a=items.filter(x=>x.name.toLowerCase().includes(q));document.getElementById("l").innerHTML=a.map(x=>"<div class=i><span>"+(x.dir?"📁":"📄")+"</span><div class=name>"+(x.dir?"<a href=# onclick=go("+JSON.stringify(x.path)+");return false>"+esc(x.name)+"</a>":"<a href=/dav"+x.path+" target=_blank>"+esc(x.name)+"</a>")+"</div><span class=meta>"+x.size+" bytes</span><button onclick=renameItem("+JSON.stringify(x.path)+","+JSON.stringify(x.name)+")>Rename</button><button class=danger onclick=removeItem("+JSON.stringify(x.path)+")>Delete</button></div>").join("")||"<p>Empty.</p>"}function go(p){cur=p;refresh()}function up(){if(cur!=="/")go(cur.substring(0,cur.lastIndexOf("/"))||"/")}async function mkdir(){let n=prompt("Folder name");if(!n)return;await api("/api/mkdir",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({path:cur+"/"+n})});refresh()}async function renameItem(p,n){let x=prompt("New name",n);if(!x)return;await api("/api/rename",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({old:p,new:p.substring(0,p.lastIndexOf("/")+1)+x})});refresh()}async function removeItem(p){if(!confirm("Delete "+p+"?"))return;await api("/api/delete",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({path:p})});refresh()}refresh()</script></body></html>`

func main(){
 _ = godotenv.Load()
 c:=loadConfig()
 s,e:=openStore(c);if e!=nil{log.Fatal(e)};defer s.db.Close()
 ctx,cancel:=context.WithCancel(context.Background());defer cancel()
 t:=&Telegram{cfg:c,store:s};go func(){if e:=t.run(ctx);e!=nil{log.Printf("telegram: %v",e)}}()
 fs:=&FS{s:s,t:t,c:newCache(c.CacheMB)}
 dav:=&webdav.Handler{Prefix:"/dav",FileSystem:fs,LockSystem:webdav.NewMemLS()}
 mux:=http.NewServeMux()
 mux.Handle("/dav/",basic(c.User,c.Pass,dav))
 mux.Handle("/api/list",basic(c.User,c.Pass,http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
  xs,e:=s.list(r.URL.Query().Get("path"));if e!=nil{http.Error(w,e.Error(),500);return}
  w.Header().Set("Content-Type","application/json")
  fmt.Fprint(w,"{\"items\":[")
  for i,x:=range xs{if i>0{fmt.Fprint(w,",")};fmt.Fprintf(w,"{\"name\":%q,\"path\":%q,\"dir\":%t,\"size\":%d}",x.Name,x.Path,x.Dir,x.Size)}
  fmt.Fprint(w,"]}")
 })))
 mux.Handle("/api/mkdir",basic(c.User,c.Pass,http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){var x struct{Path string `json:"path"`};if json.NewDecoder(r.Body).Decode(&x)!=nil{http.Error(w,"bad json",400);return};if e:=s.ensureFolder(x.Path);e!=nil{http.Error(w,e.Error(),500);return};w.WriteHeader(204)})))
 mux.Handle("/api/rename",basic(c.User,c.Pass,http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){var x struct{Old string `json:"old"`;New string `json:"new"`};if json.NewDecoder(r.Body).Decode(&x)!=nil{http.Error(w,"bad json",400);return};if e:=s.rename(x.Old,x.New);e!=nil{http.Error(w,e.Error(),500);return};w.WriteHeader(204)})))
 mux.Handle("/api/delete",basic(c.User,c.Pass,http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){var x struct{Path string `json:"path"`};if json.NewDecoder(r.Body).Decode(&x)!=nil{http.Error(w,"bad json",400);return};if e:=s.remove(x.Path);e!=nil{http.Error(w,e.Error(),500);return};w.WriteHeader(204)})))
 mux.HandleFunc("/web",func(w http.ResponseWriter,r *http.Request){w.Header().Set("Content-Type","text/html; charset=utf-8");io.WriteString(w,page)})
 mux.HandleFunc("/",func(w http.ResponseWriter,r *http.Request){http.Redirect(w,r,"/web",http.StatusFound)})
 log.Printf("WebDAV: http://0.0.0.0%s/dav",c.Addr);log.Printf("Web: http://0.0.0.0%s/web",c.Addr)
 log.Fatal(http.ListenAndServe(c.Addr,mux))
}
