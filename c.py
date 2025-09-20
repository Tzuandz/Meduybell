import os
import re
import time
import asyncio
import shutil
import unicodedata
import subprocess
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set
from urllib.parse import urlparse
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from telegram.request import HTTPXRequest
from telegram.error import RetryAfter, TimedOut, NetworkError, BadRequest

try:
    import yt_dlp
except Exception:
    raise SystemExit("pip install -U yt-dlp")

def TFILE(path: str):
    try:
        from telegram import FSInputFile
        return FSInputFile(path)
    except Exception:
        return open(path, "rb")

BOT_TOKEN = os.getenv("BOT_TOKEN", "8276989607:AAFuu_C7WzGlQSZBTTCZzus7Dfb45UnU1W8")
URL_RE = re.compile(r'(https?://\S+)')
PLATFORMS = [
    ("Auto", ["*"]),
    ("YouTube", ["youtube.com", "youtu.be"]),
    ("TikTok", ["tiktok.com", "vm.tiktok.com"]),
    ("Facebook", ["facebook.com", "fb.watch", "m.facebook.com"])
]
QUALITIES = [
    ("1","≤240p",240),
    ("2","≤360p",360),
    ("3","≤480p",480),
    ("4","≤720p",720),
    ("5","≤1080p",1080),
    ("6","≤1440p",1440),
    ("7","≤2160p",2160),
    ("8","Best",None)
]

TG_MAX_BYTES = int(os.getenv("TG_MAX_BYTES", str(2*1024*1024*1024 - 4096)))

def domain_of(u:str)->str:
    try: return urlparse(u).netloc.lower()
    except: return ""

def ffmpeg_exists()->bool:
    return shutil.which("ffmpeg") is not None

def ensure_dirs(base:str)->Dict[str,str]:
    v=os.path.join(base,"Videos"); a=os.path.join(base,"Audio")
    for d in (v,a): os.makedirs(d,exist_ok=True)
    return {"videos":v,"audio":a}

def faststart_mp4(path:str)->bool:
    if not ffmpeg_exists(): return False
    root,ext=os.path.splitext(path)
    if ext.lower()!=".mp4": return False
    tmp=root+".fast.mp4"
    r=subprocess.run(["ffmpeg","-y","-i",path,"-c","copy","-movflags","+faststart",tmp],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    if r.returncode!=0: return False
    try:
        os.replace(tmp,path)
        return True
    except Exception:
        return False

def human_size(n:int)->str:
    try:
        s=float(n)
        for unit in ["B","KB","MB","GB","TB"]:
            if s<1024: return f"{s:.1f} {unit}"
            s/=1024
        return f"{s:.1f} PB"
    except Exception:
        return "-"

def build_fmt(mode:str, q:str, platform_idx:int)->str:
    h = next((h for n,_,h in QUALITIES if n==q), None)
    def cap(expr:str)->str:
        return f"{expr}[height<={h}]" if h else expr
    prefer_mp4 = platform_idx in (1,3)
    if mode=="1":
        if prefer_mp4:
            return f"({cap('bestvideo[ext=mp4]')}/{cap('bestvideo')})+bestaudio[ext=m4a]/bestaudio/best[ext=mp4]/best"
        return f"{cap('bestvideo')}+bestaudio/best"
    if mode=="2":
        if prefer_mp4:
            return f"{cap('bestvideo[ext=mp4]')}/{cap('bestvideo')}/best"
        return cap("bestvideo")
    return "bestaudio/best"

def sanitize_filename(name:str, maxlen:int=100)->str:
    base=os.path.basename(name)
    base=unicodedata.normalize("NFKD", base)
    base=base.replace("/", "_").replace("\\", "_").replace("\n"," ").strip()
    if len(base)>maxlen:
        root,ext=os.path.splitext(base)
        base=root[:max(1,maxlen-len(ext)-1)]+"_"+ext.lstrip(".")
    return base

def ensure_mp4_ext(path:str)->str:
    root,ext=os.path.splitext(path)
    if ext.lower() in [".mp4",".mkv",".webm",".mov",".m4v",".mp3",".m4a",".ogg",".opus",".flac",".wav"]:
        return path
    new=root+".mp4"
    try:
        os.rename(path,new)
        return new
    except Exception:
        return path

@dataclass
class UserState:
    step:str="idle"
    pidx:int=-1
    urls:List[str]=field(default_factory=list)
    mode:str="1"
    q:str="8"
    outdir:str=""
    subdirs:Dict[str,str]=field(default_factory=dict)
    cookies:str=""
    proxy:str=""
    send_pref:str="doc"

class ProgressReporter:
    def __init__(self, bot, chat_id:int, message_id:int, loop:asyncio.AbstractEventLoop):
        self.bot=bot
        self.chat_id=chat_id
        self.message_id=message_id
        self.loop=loop
        self.last=time.time()-10
        self.title=""
    def hook(self, d:Dict):
        now=time.time()
        if d.get("status")=="downloading":
            tb = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            db = d.get("downloaded_bytes") or 0
            pct = f"{(db/tb*100):.1f}%" if tb else "…"
            spd = d.get("speed") or 0
            eta = d.get("eta") or 0
            txt = f"Đang tải… {pct} • {human_size(db)}/{human_size(tb)} • {human_size(spd)}/s • ETA {int(eta)}s"
            if now - self.last >= 1:
                self.last=now
                asyncio.run_coroutine_threadsafe(self.bot.edit_message_text(chat_id=self.chat_id,message_id=self.message_id,text=txt), self.loop)
        elif d.get("status")=="finished":
            fn = d.get("filename") or ""
            if fn: self.title=os.path.basename(fn)
            asyncio.run_coroutine_threadsafe(self.bot.edit_message_text(chat_id=self.chat_id,message_id=self.message_id,text="Đã tải xong. Đang xử lý…"), self.loop)

class UploadTicker:
    frames=["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
    def __init__(self, bot, chat_id:int):
        self.bot=bot
        self.chat_id=chat_id
        self.msg_id:Optional[int]=None
        self._task:Optional[asyncio.Task]=None
        self._stop=False
        self.start_ts=0
        self.attempt=1
    async def start(self, head:str):
        m=await self.bot.send_message(chat_id=self.chat_id,text=head)
        self.msg_id=m.message_id
        self.start_ts=time.time()
        self._stop=False
        self._task=asyncio.create_task(self._run())
    async def again(self):
        self.attempt+=1
        self.start_ts=time.time()
    async def _run(self):
        i=0
        while not self._stop:
            elapsed=int(time.time()-self.start_ts)
            try:
                await self.bot.edit_message_text(chat_id=self.chat_id,message_id=self.msg_id,text=f"{self.frames[i%len(self.frames)]} Đang gửi dạng file… {elapsed}s • lần {self.attempt}")
            except Exception:
                pass
            i+=1
            await asyncio.sleep(2)
    async def stop(self, tail:str):
        self._stop=True
        if self._task:
            try:
                await self._task
            except Exception:
                pass
        if self.msg_id:
            try:
                await self.bot.edit_message_text(chat_id=self.chat_id,message_id=self.msg_id,text=tail)
            except Exception:
                pass

class Bot:
    def __init__(self):
        self.users:Dict[int,UserState]={}

    def get(self,uid:int)->UserState:
        s=self.users.get(uid)
        if not s:
            base=os.path.join(os.getcwd(),"Downloads",f"user_{uid}")
            os.makedirs(base,exist_ok=True)
            s=UserState(outdir=base,subdirs=ensure_dirs(base))
            self.users[uid]=s
        return s

    def reset(self,uid:int):
        base=os.path.join(os.getcwd(),"Downloads",f"user_{uid}")
        os.makedirs(base,exist_ok=True)
        self.users[uid]=UserState(outdir=base,subdirs=ensure_dirs(base))

    def kb_platform(self)->InlineKeyboardMarkup:
        rows=[[InlineKeyboardButton(f"{i} {name}",callback_data=f"wiz:p:{i}")] for i,(name,_) in enumerate(PLATFORMS)]
        return InlineKeyboardMarkup(rows)

    def kb_mode(self)->InlineKeyboardMarkup:
        rows=[
            [InlineKeyboardButton("1 Video+Audio",callback_data="wiz:m:1")],
            [InlineKeyboardButton("2 Video-không-âm",callback_data="wiz:m:2")],
            [InlineKeyboardButton("3 MP3",callback_data="wiz:m:3")]
        ]
        return InlineKeyboardMarkup(rows)

    def kb_quality(self)->InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[InlineKeyboardButton(f"{n} {name}",callback_data=f"wiz:q:{n}")] for n,name,_ in QUALITIES])

    async def cmd_start(self,update:Update,context:ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Dùng /menu để bắt đầu.")

    async def cmd_menu(self,update:Update,context:ContextTypes.DEFAULT_TYPE):
        uid=update.effective_user.id
        self.reset(uid)
        await update.message.reply_text("Chọn nền tảng:",reply_markup=self.kb_platform())

    async def cmd_reset(self,update:Update,context:ContextTypes.DEFAULT_TYPE):
        uid=update.effective_user.id
        self.reset(uid)
        await update.message.reply_text("Đã reset.")

    async def cmd_proxy(self,update:Update,context:ContextTypes.DEFAULT_TYPE):
        uid=update.effective_user.id
        s=self.get(uid)
        parts=(update.message.text or "").split(maxsplit=1)
        s.proxy=parts[1].strip() if len(parts)>1 else ""
        await update.message.reply_text("OK.")

    async def on_doc(self,update:Update,context:ContextTypes.DEFAULT_TYPE):
        uid=update.effective_user.id
        s=self.get(uid)
        doc=update.message.document
        if not doc: return
        fn=(doc.file_name or "file.bin").lower()
        path=os.path.join(s.outdir,fn)
        f=await context.bot.get_file(doc.file_id)
        await f.download_to_drive(path)
        if "cookie" in fn or "cookies" in fn:
            s.cookies=path
            await update.message.reply_text("Đã nhận cookies.")
        else:
            await update.message.reply_text("Đã nhận file.")

    async def on_text(self,update:Update,context:ContextTypes.DEFAULT_TYPE):
        uid=update.effective_user.id
        s=self.get(uid)
        text=update.message.text or ""
        urls=[u.rstrip(").,]") for u in URL_RE.findall(text)]
        if s.step=="await_link":
            if not urls:
                self.reset(uid)
                await update.message.reply_text("Sai link. /menu làm lại.")
                return
            if s.pidx>0:
                doms=PLATFORMS[s.pidx][1]
                urls=[u for u in urls if any(d in domain_of(u) for d in doms)] or urls
            s.urls=urls
            s.step="await_mode"
            await update.message.reply_text("Chọn chế độ:",reply_markup=self.kb_mode())
            return
        await update.message.reply_text("Dùng /menu để bắt đầu.")

    async def on_cb(self,update:Update,context:ContextTypes.DEFAULT_TYPE):
        q=update.callback_query
        await q.answer()
        uid=q.from_user.id
        s=self.get(uid)
        parts=(q.data or "").split(":")
        if len(parts)<3 or parts[0]!="wiz": return
        typ,val=parts[1],parts[2]
        if typ=="p":
            s.pidx=int(val); s.step="await_link"
            await q.edit_message_text("Dán link (1 hoặc nhiều).")
            return
        if typ=="m" and s.step in ("await_mode","await_link","idle"):
            s.mode=val
            if s.mode=="3":
                s.q="8"
                await q.edit_message_text("Bắt đầu tải…")
                await self.process_download(update,context,s)
                self.reset(uid)
                return
            s.step="await_q"
            await q.edit_message_text("Chọn chất lượng:",reply_markup=self.kb_quality())
            return
        if typ=="q" and s.step=="await_q":
            s.q=val
            await q.edit_message_text("Bắt đầu tải…")
            await self.process_download(update,context,s)
            self.reset(uid)
            return

    def ydl_opts(self,s:UserState, reporter:Optional[ProgressReporter]=None)->Dict:
        merge = "mp4" if s.mode=="1" else ("mkv" if s.mode=="2" else "mp3")
        outtmpl = {
            "default": os.path.join(s.subdirs["audio" if s.mode=="3" else "videos"], "%(title).80s [%(id)s]-%(extractor)s.%(ext)s")
        }
        opts = {
            "format": build_fmt(s.mode,s.q,s.pidx),
            "outtmpl": outtmpl,
            "merge_output_format": merge,
            "retries": 20,
            "fragment_retries": 20,
            "concurrent_fragment_downloads": 5,
            "ignoreerrors": True,
            "prefer_ffmpeg": True,
            "noplaylist": False,
            "quiet": True,
            "no_warnings": True,
            "noprogress": True
        }
        if reporter:
            opts["progress_hooks"]=[reporter.hook]
        if s.mode=="3":
            opts["postprocessors"]=[{"key":"FFmpegExtractAudio","preferredcodec":"mp3","preferredquality":"320"}]
        if s.cookies: opts["cookiefile"]=s.cookies
        if s.proxy: opts["proxy"]=s.proxy
        return opts

    async def send_as_file_forever(self, bot, chat_id:int, path:str):
        size=os.path.getsize(path)
        if size > TG_MAX_BYTES:
            await bot.send_message(chat_id=chat_id,text=f"Tệp {os.path.basename(path)} lớn {human_size(size)} vượt giới hạn Telegram.")
            return False
        ticker=UploadTicker(bot, chat_id)
        await ticker.start(f"Chuẩn bị gửi: {os.path.basename(path)} • {human_size(size)}")
        attempt=0
        caption=os.path.basename(path)
        while True:
            attempt+=1
            try:
                await bot.send_document(chat_id=chat_id,document=TFILE(path),caption=caption)
                await ticker.stop("Đã gửi xong.")
                return True
            except RetryAfter as e:
                await asyncio.sleep(int(getattr(e,"retry_after",5)))
                await ticker.again()
                continue
            except (TimedOut, NetworkError, BadRequest, Exception):
                await asyncio.sleep(3)
                await ticker.again()
                continue

    async def process_download(self,update:Update,context:ContextTypes.DEFAULT_TYPE,s:UserState):
        chat_id=update.effective_chat.id
        before_v=self.snapshot(s.subdirs["videos"]); before_a=self.snapshot(s.subdirs["audio"])
        loop=asyncio.get_running_loop()
        for u in s.urls:
            m=await context.bot.send_message(chat_id=chat_id,text="Bắt đầu tải…")
            reporter=ProgressReporter(context.bot, chat_id, m.message_id, loop)
            opts=self.ydl_opts(s, reporter)
            await asyncio.to_thread(self.download_one,u,opts)
        new_v=sorted(self.snapshot(s.subdirs["videos"])-before_v)
        new_a=sorted(self.snapshot(s.subdirs["audio"])-before_a)

        for f in new_v:
            p=os.path.join(s.subdirs["videos"],f)
            if s.mode=="2" and os.path.isfile(p):
                await asyncio.to_thread(self.strip_audio,p)
            if p.lower().endswith(".mp4"):
                await asyncio.to_thread(faststart_mp4,p)
            np=ensure_mp4_ext(p)
            if np!=p: p=np
            new_name=sanitize_filename(os.path.basename(p))
            new_path=os.path.join(s.subdirs["videos"],new_name)
            if new_path!=p:
                try: os.replace(p,new_path); p=new_path
                except Exception: pass
        for f in new_a:
            p=os.path.join(s.subdirs["audio"],f)
            new_name=sanitize_filename(os.path.basename(p))
            new_path=os.path.join(s.subdirs["audio"],new_name)
            if new_path!=p:
                try: os.replace(p,new_path)
                except Exception: pass

        sent=0
        for f in sorted(self.snapshot(s.subdirs["videos"])-before_v):
            p=os.path.join(s.subdirs["videos"],f)
            if not os.path.isfile(p) or os.path.getsize(p)<=0: continue
            ok=await self.send_as_file_forever(context.bot, chat_id, p)
            if ok: sent+=1
        for f in sorted(self.snapshot(s.subdirs["audio"])-before_a):
            p=os.path.join(s.subdirs["audio"],f)
            if not os.path.isfile(p) or os.path.getsize(p)<=0: continue
            ok=await self.send_as_file_forever(context.bot, chat_id, p)
            if ok: sent+=1
        await context.bot.send_message(chat_id=chat_id,text=f"Xong ({sent} tệp).")

    def strip_audio(self,path:str)->bool:
        if not ffmpeg_exists(): return False
        root,ext=os.path.splitext(path)
        tmp=root+".noaudio"+ext
        r=subprocess.run(["ffmpeg","-y","-i",path,"-c","copy","-an","-movflags","+faststart",tmp],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        if r.returncode!=0: return False
        try:
            os.replace(tmp,path)
            return True
        except Exception:
            return False

    def snapshot(self,dirpath:str)->Set[str]:
        try: return {f for f in os.listdir(dirpath) if os.path.isfile(os.path.join(dirpath,f))}
        except Exception: return set()

    def download_one(self,url:str,ydl_opts:Dict)->int:
        with yt_dlp.YoutubeDL(dict(ydl_opts)) as ydl:
            return ydl.download([url])

def main():
    if not BOT_TOKEN:
        raise SystemExit("Set BOT_TOKEN")
    req=HTTPXRequest(connect_timeout=60,read_timeout=604800,write_timeout=604800,pool_timeout=600)
    app=ApplicationBuilder().token(BOT_TOKEN).request(req).build()
    b=Bot()
    app.add_handler(CommandHandler("start",b.cmd_start))
    app.add_handler(CommandHandler("menu",b.cmd_menu))
    app.add_handler(CommandHandler("reset",b.cmd_reset))
    app.add_handler(CommandHandler("proxy",b.cmd_proxy))
    app.add_handler(CallbackQueryHandler(b.on_cb))
    app.add_handler(MessageHandler(filters.Document.ALL,b.on_doc))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,b.on_text))
    app.run_polling()

if __name__=="__main__":
    main()