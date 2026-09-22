package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
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

type Telegram struct {
	cfg Config
	store *Store
	client *telegram.Client
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
	if filepath.Ext(name)=="" { if ex:=mime.ExtensionsByType(mt);len(ex)>0{name+=ex[0]} }
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
func(f *FS)Mkdir(context.Context,string,os.FileMode)error{return errors.New("MKCOL disabled in v1")}
func(f *FS)Stat(_ context.Context,n string)(os.FileInfo,error){i,e:=f.s.stat(n);if e!=nil{return nil,e};return fileInfo{i.Name,i.Size,i.Dir,i.Mod},nil}
func(f *FS)RemoveAll(context.Context,string)error{return errors.New("DELETE disabled in v1")}
func(f *FS)Rename(context.Context,string,string)error{return errors.New("MOVE disabled in v1")}
func(f *FS)OpenFile(ctx context.Context,n string,_ int,_ os.FileMode)(webdav.File,error){i,e:=f.s.stat(n);if e!=nil{return nil,e};if i.Dir{xs,e:=f.s.list(n);if e!=nil{return nil,e};return &dirFile{fileInfo{i.Name,0,true,i.Mod},xs,0},nil};if f.t.client==nil{return nil,errors.New("Telegram not connected")};l,e:=decodeLocation(i.Loc);if e!=nil{return nil,e};return &rangeFile{ctx,f.t.client,l,i.Size,0,i.Name,f.t.cfg.Chunk,f.c},nil}

func basic(user,pass string,h http.Handler)http.Handler{return http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){u,p,ok:=r.BasicAuth();if !ok||u!=user||p!=pass{w.Header().Set("WWW-Authenticate","Basic realm="Tg-webdav"");http.Error(w,"unauthorized",401);return};h.ServeHTTP(w,r)})}

const page = "<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>Tg-webdav</title><style>body{font-family:system-ui;background:#0b1020;color:#eef;padding:20px;max-width:1000px;margin:auto}.i{padding:12px;margin:7px 0;background:#111a2d;border-radius:10px}.m{color:#8895ad}</style></head><body><h1>Telegram WebDAV</h1><p id="p"></p><div id="l"></div><script>let p='/general';async function load(){let r=await fetch('/api/list?path='+encodeURIComponent(p));if(r.status==401){alert('Use WebDAV Basic Auth credentials.');return}let j=await r.json();document.getElementById('p').textContent=p;document.getElementById('l').innerHTML=(j.items||[]).map(x=>'<div class=i>'+ (x.dir?'📁':'📄') +' '+x.name+' <span class=m>'+x.size+' bytes</span></div>').join('')||'Empty'}load()</script></body></html>"

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
	mux.HandleFunc("/api/list",func(w http.ResponseWriter,r *http.Request){xs,e:=s.list(r.URL.Query().Get("path"));if e!=nil{http.Error(w,e.Error(),500);return};w.Header().Set("Content-Type","application/json");fmt.Fprint(w,"{"items":[");for i,x:=range xs{if i>0{fmt.Fprint(w,",")};fmt.Fprintf(w,"{"name":%q,"path":%q,"dir":%t,"size":%d}",x.Name,x.Path,x.Dir,x.Size)};fmt.Fprint(w,"]}")})
	mux.HandleFunc("/web",func(w http.ResponseWriter,r *http.Request){w.Header().Set("Content-Type","text/html; charset=utf-8");io.WriteString(w,page)})
	mux.HandleFunc("/",func(w http.ResponseWriter,r *http.Request){http.Redirect(w,r,"/web",http.StatusFound)})
	log.Printf("WebDAV: http://0.0.0.0%s/dav",c.Addr);log.Printf("Web: http://0.0.0.0%s/web",c.Addr)
	log.Fatal(http.ListenAndServe(c.Addr,mux))
}
