import os
import re
import sys
import json
import time
import csv
import shutil
import queue
import threading
import subprocess
from dataclasses import dataclass
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog

# ---- Mutagen (MP3 ID3 + M4A MP4-tags) ----
try:
    from mutagen.id3 import (
        ID3, ID3NoHeaderError,
        TIT2, TPE1, TALB, TRCK, TDRC, TCON, COMM, APIC
    )
    from mutagen.mp4 import MP4, MP4Cover
    MUTAGEN_OK = True
except Exception:
    MUTAGEN_OK = False

# ---- Pillow (för JPG preview) ----
try:
    from PIL import Image, ImageTk
    PIL_OK = True
except Exception:
    PIL_OK = False

APP_NAME = "Media Manager (Finder-style)"
CONFIG_FILENAME = "media_manager_config.json"

# ---- Filtyper ----
VIDEO_EXTS = {".mp4", ".mkv", ".mov", ".m4v", ".avi", ".wmv", ".flv", ".webm"}
AUDIO_EXTS = {".m4a", ".mp3", ".ogg", ".wav", ".flac", ".aac", ".alac", ".aiff"}

EXPORT_VIDEO_FORMATS = ["mp4", "mkv", "mov"]
EXPORT_AUDIO_FORMATS = ["m4a", "mp3", "ogg"]

# ---------------- Helpers ----------------
def human_size(num_bytes: int) -> str:
    if num_bytes < 0:
        return ""
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(num_bytes)
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024
        i += 1
    if i == 0:
        return f"{int(n)} {units[i]}"
    return f"{n:.1f} {units[i]}"

def human_time(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return ""
    seconds = int(round(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"

def safe_filename(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[\/\:\*\?\"<>\|]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

def title_case_words(s: str) -> str:
    parts = re.split(r"(\s+)", s)
    out = []
    for p in parts:
        if p.isspace():
            out.append(p)
        else:
            out.append(p[:1].upper() + p[1:].lower() if p else p)
    return "".join(out)

def remove_words_anywhere(filename_stem: str, words_to_remove: list[str]) -> str:
    s = filename_stem
    for w in words_to_remove:
        w = w.strip()
        if not w:
            continue
        pattern = r"(?i)\b" + re.escape(w) + r"\b"
        s = re.sub(pattern, "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\s*[\-\_\.\,]\s*", " ", s).strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def read_bytes(path: Path) -> bytes:
    return path.read_bytes()

def guess_mime_from_ext(path: Path) -> str:
    ext = path.suffix.lower()
    if ext == ".png":
        return "image/png"
    return "image/jpeg"

# ---------------- Bundled ffmpeg/ffprobe ----------------
def resolve_tool(exe: str) -> str | None:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        base = Path(sys._MEIPASS)
        cand = base / "tools" / exe
        if cand.exists():
            return str(cand)
    base = Path(__file__).resolve().parent
    cand = base / "tools" / exe
    if cand.exists():
        return str(cand)
    return shutil.which(exe)

def tool_ok(exe_path: str | None, exe_name: str) -> tuple[bool, str]:
    if not exe_path:
        return (False, f"{exe_name} hittades inte.")
    try:
        p = subprocess.run([exe_path, "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5)
        if p.returncode == 0:
            return (True, exe_path)
        return (False, f"{exe_name} kunde inte köras (returncode {p.returncode}).")
    except Exception as e:
        return (False, f"{exe_name} kunde inte köras: {e}")

# ---------------- ffprobe ----------------
def run_ffprobe_json(ffprobe_path: str, path: str) -> dict | None:
    cmd = [ffprobe_path, "-v", "error", "-print_format", "json", "-show_format", "-show_streams", path]
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if p.returncode != 0:
            return None
        return json.loads(p.stdout)
    except Exception:
        return None

def extract_metadata(ffprobe_path: str | None, path: str) -> dict:
    info = {"duration": None, "width": None, "height": None, "genre": None}
    if not ffprobe_path:
        return info

    data = run_ffprobe_json(ffprobe_path, path)
    if not data:
        return info

    fmt = data.get("format", {}) or {}
    tags = (fmt.get("tags", {}) or {})
    genre = tags.get("genre") or tags.get("GENRE")
    if isinstance(genre, str) and genre.strip():
        info["genre"] = genre.strip()

    dur = fmt.get("duration")
    try:
        info["duration"] = float(dur) if dur is not None else None
    except Exception:
        info["duration"] = None

    streams = data.get("streams", []) or []
    for st in streams:
        if st.get("codec_type") == "video":
            w = st.get("width")
            h = st.get("height")
            try:
                info["width"] = int(w) if w is not None else None
                info["height"] = int(h) if h is not None else None
            except Exception:
                info["width"], info["height"] = None, None
            break

    return info

# ---------------- ffmpeg ----------------
def run_ffmpeg_with_progress(cmd: list[str], progress_cb=None) -> tuple[bool, str]:
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
    except Exception as e:
        return (False, f"Kunde inte starta ffmpeg: {e}")

    total_ms = None
    last_update = time.time()
    stderr_acc = []

    def read_stderr():
        try:
            for line in p.stderr:
                stderr_acc.append(line)
        except Exception:
            pass

    threading.Thread(target=read_stderr, daemon=True).start()

    try:
        for line in p.stdout:
            line = line.strip()
            if not line or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip()

            if k == "duration_ms":
                try:
                    total_ms = int(v)
                except Exception:
                    total_ms = None

            if k == "out_time_ms":
                pct = None
                if total_ms and total_ms > 0:
                    try:
                        out_ms = int(v)
                        pct = max(0.0, min(100.0, (out_ms / total_ms) * 100.0))
                    except Exception:
                        pct = None

                if progress_cb and (time.time() - last_update) > 0.12:
                    progress_cb(pct, "Bearbetar…")
                    last_update = time.time()

            if k == "progress" and v == "end":
                if progress_cb:
                    progress_cb(100.0, "Klar")
    except Exception:
        pass

    rc = p.wait()
    if rc == 0:
        return (True, "OK")
    tail = "".join(stderr_acc[-30:]).strip()
    return (False, tail or "ffmpeg misslyckades (ingen stderr).")

def ffmpeg_write_genre_inplace(ffmpeg_path: str | None, src_path: str, genre_str: str, progress_cb=None) -> tuple[bool, str]:
    if not ffmpeg_path:
        return (False, "ffmpeg saknas i appen (tools/ffmpeg).")

    src = Path(src_path)
    tmp = src.with_name(src.stem + ".__tmp__" + src.suffix)

    cmd = [ffmpeg_path, "-y", "-i", str(src), "-map", "0", "-c", "copy",
           "-metadata", f"genre={genre_str}",
           "-progress", "pipe:1", "-nostats", str(tmp)]

    ok, msg = run_ffmpeg_with_progress(cmd, progress_cb=progress_cb)
    if not ok:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        return (False, msg)

    try:
        backup = src.with_name(src.stem + ".__bak__" + src.suffix)
        if backup.exists():
            backup.unlink()
        src.rename(backup)
        tmp.rename(src)
        backup.unlink(missing_ok=True)
    except Exception as e:
        return (False, f"Kunde inte ersätta originalfilen: {e}")

    return (True, "OK")

def ffmpeg_export(ffmpeg_path: str | None, src_path: str, dst_path: str, kind: str, progress_cb=None) -> tuple[bool, str]:
    if not ffmpeg_path:
        return (False, "ffmpeg saknas i appen (tools/ffmpeg).")

    src = Path(src_path)
    dst = Path(dst_path)
    ext = dst.suffix.lower()

    if kind == "video":
        cmd = [
            ffmpeg_path, "-y",
            "-i", str(src),
            "-map", "0:v:0", "-map", "0:a?",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", "20",
            "-c:a", "aac",
            "-b:a", "192k",
            "-progress", "pipe:1",
            "-nostats",
            str(dst)
        ]
        return run_ffmpeg_with_progress(cmd, progress_cb=progress_cb)

    if ext == ".m4a":
        cmd = [ffmpeg_path, "-y", "-i", str(src), "-vn", "-c:a", "aac", "-b:a", "192k",
               "-progress", "pipe:1", "-nostats", str(dst)]
    elif ext == ".mp3":
        cmd = [ffmpeg_path, "-y", "-i", str(src), "-vn", "-c:a", "libmp3lame", "-q:a", "2",
               "-progress", "pipe:1", "-nostats", str(dst)]
    elif ext == ".ogg":
        cmd = [ffmpeg_path, "-y", "-i", str(src), "-vn", "-c:a", "libvorbis", "-q:a", "6",
               "-progress", "pipe:1", "-nostats", str(dst)]
    else:
        return (False, f"Okänt ljudformat: {ext}")

    return run_ffmpeg_with_progress(cmd, progress_cb=progress_cb)

# ---------------- Taggar (MP3 + M4A) med omslag ----------------
def tags_read_mp3(path: Path) -> dict:
    if not MUTAGEN_OK:
        return {}
    try:
        tags = ID3(str(path))
    except ID3NoHeaderError:
        return {}
    except Exception:
        return {}

    def get_text(frame_id):
        f = tags.getall(frame_id)
        if not f:
            return ""
        fr = f[0]
        try:
            return str(fr.text[0]) if getattr(fr, "text", None) else ""
        except Exception:
            return ""

    out = {
        "title": get_text("TIT2"),
        "artist": get_text("TPE1"),
        "album": get_text("TALB"),
        "track": get_text("TRCK"),
        "year": get_text("TDRC"),
        "genre": get_text("TCON"),
        "comment": "",
        "has_cover": False,
        "cover_bytes": None,
        "cover_mime": None,
    }

    comm = tags.getall("COMM")
    if comm:
        try:
            out["comment"] = str(comm[0].text[0]) if comm[0].text else ""
        except Exception:
            out["comment"] = ""

    apic = tags.getall("APIC")
    if apic:
        try:
            out["has_cover"] = True
            out["cover_bytes"] = apic[0].data
            out["cover_mime"] = apic[0].mime
        except Exception:
            pass

    return out

def tags_write_mp3(path: Path, fields: dict, cover: dict | None) -> tuple[bool, str]:
    if not MUTAGEN_OK:
        return (False, "Mutagen saknas (pip install mutagen).")
    try:
        try:
            tags = ID3(str(path))
        except ID3NoHeaderError:
            tags = ID3()

        def put(frame_id: str, frame_obj, value: str):
            value = (value or "").strip()
            if value:
                tags.setall(frame_id, [frame_obj])
            else:
                tags.delall(frame_id)

        put("TIT2", TIT2(encoding=3, text=(fields.get("title", "") or "").strip()), fields.get("title", ""))
        put("TPE1", TPE1(encoding=3, text=(fields.get("artist", "") or "").strip()), fields.get("artist", ""))
        put("TALB", TALB(encoding=3, text=(fields.get("album", "") or "").strip()), fields.get("album", ""))
        put("TRCK", TRCK(encoding=3, text=(fields.get("track", "") or "").strip()), fields.get("track", ""))
        put("TDRC", TDRC(encoding=3, text=(fields.get("year", "") or "").strip()), fields.get("year", ""))
        put("TCON", TCON(encoding=3, text=(fields.get("genre", "") or "").strip()), fields.get("genre", ""))

        comm_val = (fields.get("comment", "") or "").strip()
        if comm_val:
            tags.setall("COMM", [COMM(encoding=3, lang="eng", desc="", text=comm_val)])
        else:
            tags.delall("COMM")

        if cover is not None:
            if cover.get("action") == "remove":
                tags.delall("APIC")
            elif cover.get("action") == "set":
                img_bytes = cover.get("bytes")
                mime = cover.get("mime") or "image/jpeg"
                if img_bytes:
                    tags.delall("APIC")
                    tags.add(APIC(encoding=3, mime=mime, type=3, desc="Cover", data=img_bytes))

        tags.save(str(path))
        return (True, "OK")
    except Exception as e:
        return (False, f"Kunde inte skriva MP3-taggar: {e}")

def tags_remove_all_mp3(path: Path) -> tuple[bool, str]:
    if not MUTAGEN_OK:
        return (False, "Mutagen saknas (pip install mutagen).")
    try:
        tags = ID3(str(path))
        tags.delete(str(path))
        return (True, "OK")
    except ID3NoHeaderError:
        return (True, "Ingen ID3 fanns (redan tomt).")
    except Exception as e:
        return (False, f"Kunde inte radera ID3: {e}")

def tags_read_m4a(path: Path) -> dict:
    if not MUTAGEN_OK:
        return {}
    try:
        m = MP4(str(path))
    except Exception:
        return {}

    def get1(key: str) -> str:
        if not m.tags:
            return ""
        v = m.tags.get(key)
        if not v:
            return ""
        try:
            return str(v[0])
        except Exception:
            return ""

    track = ""
    if m.tags and "trkn" in m.tags and m.tags["trkn"]:
        try:
            tn, tt = m.tags["trkn"][0]
            track = f"{tn}/{tt}" if tt else str(tn)
        except Exception:
            track = ""

    cover_bytes = None
    cover_mime = None
    has_cover = False
    if m.tags and "covr" in m.tags and m.tags["covr"]:
        try:
            cov = m.tags["covr"][0]
            has_cover = True
            cover_bytes = bytes(cov)
            try:
                if getattr(cov, "imageformat", None) == MP4Cover.FORMAT_PNG:
                    cover_mime = "image/png"
                else:
                    cover_mime = "image/jpeg"
            except Exception:
                cover_mime = "image/jpeg"
        except Exception:
            pass

    return {
        "title": get1("©nam"),
        "artist": get1("©ART"),
        "album": get1("©alb"),
        "track": track,
        "year": get1("©day"),
        "genre": get1("©gen"),
        "comment": get1("©cmt"),
        "has_cover": has_cover,
        "cover_bytes": cover_bytes,
        "cover_mime": cover_mime,
    }

def _parse_track(track_str: str) -> tuple[int, int]:
    track_str = (track_str or "").strip()
    if not track_str:
        return (0, 0)
    if "/" in track_str:
        a, b = track_str.split("/", 1)
        try:
            return (int(a.strip()), int(b.strip()))
        except Exception:
            return (0, 0)
    try:
        return (int(track_str), 0)
    except Exception:
        return (0, 0)

def tags_write_m4a(path: Path, fields: dict, cover: dict | None) -> tuple[bool, str]:
    if not MUTAGEN_OK:
        return (False, "Mutagen saknas (pip install mutagen).")
    try:
        m = MP4(str(path))
        if m.tags is None:
            m.add_tags()

        def put(key: str, val: str):
            val = (val or "").strip()
            if val:
                m.tags[key] = [val]
            else:
                if key in m.tags:
                    del m.tags[key]

        put("©nam", fields.get("title", ""))
        put("©ART", fields.get("artist", ""))
        put("©alb", fields.get("album", ""))
        put("©day", fields.get("year", ""))
        put("©gen", fields.get("genre", ""))
        put("©cmt", fields.get("comment", ""))

        tr = (fields.get("track", "") or "").strip()
        if tr:
            tn, tt = _parse_track(tr)
            if tn:
                m.tags["trkn"] = [(tn, tt)]
        else:
            if "trkn" in m.tags:
                del m.tags["trkn"]

        if cover is not None:
            if cover.get("action") == "remove":
                if "covr" in m.tags:
                    del m.tags["covr"]
            elif cover.get("action") == "set":
                img_bytes = cover.get("bytes")
                mime = cover.get("mime") or "image/jpeg"
                if img_bytes:
                    if mime == "image/png":
                        m.tags["covr"] = [MP4Cover(img_bytes, imageformat=MP4Cover.FORMAT_PNG)]
                    else:
                        m.tags["covr"] = [MP4Cover(img_bytes, imageformat=MP4Cover.FORMAT_JPEG)]

        m.save()
        return (True, "OK")
    except Exception as e:
        return (False, f"Kunde inte skriva M4A-taggar: {e}")

def tags_remove_all_m4a(path: Path) -> tuple[bool, str]:
    if not MUTAGEN_OK:
        return (False, "Mutagen saknas (pip install mutagen).")
    try:
        m = MP4(str(path))
        if m.tags:
            m.tags.clear()
            m.save()
        return (True, "OK")
    except Exception as e:
        return (False, f"Kunde inte radera M4A-taggar: {e}")

# ---------------- Data ----------------
@dataclass
class FileItem:
    path: Path
    kind: str  # "video"/"audio"
    size_bytes: int
    duration: float | None
    width: int | None
    height: int | None
    existing_genre: str | None
    selected_genres: set[str]
    new_name_stem: str | None

# ---------------- Config ----------------
def load_config(config_path: Path) -> dict:
    if config_path.exists():
        try:
            return json.loads(config_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_config(config_path: Path, cfg: dict) -> None:
    try:
        config_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def default_config() -> dict:
    return {
        "video_genres": ["Action", "Drama", "Komedi", "Thriller", "Sci-Fi", "Dokumentär"],
        "audio_genres": ["Pop", "Rock", "Hip-Hop", "Elektroniskt", "Jazz", "Klassiskt"],
        "last_folder": ""
    }

# ---------------- App ----------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1300x860")
        self.minsize(1080, 720)

        self.work_q = queue.Queue()
        self.items: list[FileItem] = []
        self.item_by_iid: dict[str, FileItem] = {}
        self.iid_by_path: dict[str, str] = {}
        self.current_folder: Path | None = None

        self.config_path = Path.home() / CONFIG_FILENAME
        self.cfg = default_config()
        self.cfg.update(load_config(self.config_path))

        self.ffmpeg_path = resolve_tool("ffmpeg")
        self.ffprobe_path = resolve_tool("ffprobe")
        self.ffmpeg_ok, _ = tool_ok(self.ffmpeg_path, "ffmpeg")
        self.ffprobe_ok, _ = tool_ok(self.ffprobe_path, "ffprobe")

        self.pending_cover_action = None
        self._cover_preview_imgtk = None

        self._build_ui()
        self._poll_queue()
        self._update_tool_status_ui()

        last = self.cfg.get("last_folder") or ""
        if last and Path(last).exists():
            self.load_folder(Path(last))

        if not MUTAGEN_OK:
            messagebox.showwarning(
                "Tagg-redigering saknas",
                "Mutagen är inte installerat, så Taggar-fliken fungerar inte.\n\n"
                "Installera med:\npython3 -m pip install mutagen"
            )

    # ---- UI ----
    def _build_ui(self):
        top = ttk.Frame(self, padding=8)
        top.pack(fill="x")

        ttk.Button(top, text="Välj mapp…", command=self.on_choose_folder).pack(side="left")
        ttk.Button(top, text="Uppdatera", command=self.on_refresh).pack(side="left", padx=6)
        ttk.Button(top, text="CSV-rename…", command=self.on_csv_rename).pack(side="left", padx=(18, 0))

        self.lbl_folder = ttk.Label(top, text="Ingen mapp vald", width=70)
        self.lbl_folder.pack(side="left", padx=10)

        self.var_tools = tk.StringVar(value="")
        ttk.Label(top, textvariable=self.var_tools).pack(side="right")

        main = ttk.Panedwindow(self, orient="horizontal")
        main.pack(fill="both", expand=True)

        left = ttk.Frame(main, padding=8)
        right = ttk.Frame(main, padding=8)
        main.add(left, weight=4)
        main.add(right, weight=2)

        cols = ("name", "size", "duration", "dims", "genres", "path")
        self.tree = ttk.Treeview(left, columns=cols, show="headings", selectmode="extended")
        for c, t in [("name","Namn"),("size","Storlek"),("duration","Speltid"),("dims","Dimensioner"),
                    ("genres","Genrer"),("path","Sökväg")]:
            self.tree.heading(c, text=t)

        self.tree.column("name", width=260, anchor="w")
        self.tree.column("size", width=90, anchor="e")
        self.tree.column("duration", width=80, anchor="e")
        self.tree.column("dims", width=110, anchor="center")
        self.tree.column("genres", width=220, anchor="w")
        self.tree.column("path", width=470, anchor="w")

        vsb = ttk.Scrollbar(left, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(left, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscroll=vsb.set, xscroll=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        left.rowconfigure(0, weight=1)
        left.columnconfigure(0, weight=1)

        self.tree.bind("<<TreeviewSelect>>", self.on_tree_select)
        self.tree.bind("<Double-1>", self.on_double_click)

        nb = ttk.Notebook(right)
        nb.pack(fill="both", expand=True)

        tab_rename = ttk.Frame(nb, padding=10)
        tab_genres = ttk.Frame(nb, padding=10)
        tab_tags = ttk.Frame(nb, padding=10)
        tab_save = ttk.Frame(nb, padding=10)

        nb.add(tab_rename, text="Namn")
        nb.add(tab_genres, text="Genrer")
        nb.add(tab_tags, text="Taggar (MP3 + M4A)")
        nb.add(tab_save, text="Spara / Export")

        # ---- Rename tab ----
        ttk.Label(tab_rename, text="Ändra filnamn för markerade filer", font=("Helvetica", 12, "bold")).pack(anchor="w")
        frm_newname = ttk.LabelFrame(tab_rename, text="Ge helt nytt namn (valfritt)")
        frm_newname.pack(fill="x", pady=8)
        ttk.Label(frm_newname, text="Om du fyller i: alla markerade får detta namn + löpnummer.").pack(anchor="w")
        self.var_newname = tk.StringVar(value="")
        ttk.Entry(frm_newname, textvariable=self.var_newname).pack(fill="x", pady=4)

        frm_remove = ttk.LabelFrame(tab_rename, text="Ta bort ord från filnamn")
        frm_remove.pack(fill="x", pady=8)
        ttk.Label(frm_remove, text="Skriv ord separerade med komma (tas bort oavsett var de ligger).").pack(anchor="w")
        self.var_remove_words = tk.StringVar(value="")
        ttk.Entry(frm_remove, textvariable=self.var_remove_words).pack(fill="x", pady=4)

        frm_case = ttk.LabelFrame(tab_rename, text="Skiftläge")
        frm_case.pack(fill="x", pady=8)
        self.var_case = tk.StringVar(value="nochange")
        ttk.Radiobutton(frm_case, text="Ingen ändring", value="nochange", variable=self.var_case).pack(anchor="w")
        ttk.Radiobutton(frm_case, text="Första bokstav stor i varje ord (Title Case)", value="title", variable=self.var_case).pack(anchor="w")
        ttk.Radiobutton(frm_case, text="Allt gemener", value="lower", variable=self.var_case).pack(anchor="w")

        btns = ttk.Frame(tab_rename)
        btns.pack(fill="x", pady=10)
        ttk.Button(btns, text="Förhandsvisa på markerade", command=self.on_preview_rename).pack(side="left")
        ttk.Button(btns, text="Applicera namnändring (i listan)", command=self.on_apply_rename_to_items).pack(side="left", padx=8)

        self.txt_preview = tk.Text(tab_rename, height=12, wrap="none")
        self.txt_preview.pack(fill="both", expand=True, pady=6)

        # ---- Genres tab ----
        ttk.Label(tab_genres, text="Genrer per fil (väljs per filtyp)", font=("Helvetica", 12, "bold")).pack(anchor="w")
        self.lbl_sel = ttk.Label(tab_genres, text="Markerad fil: (ingen)")
        self.lbl_sel.pack(anchor="w", pady=(6, 2))

        frm_exist = ttk.LabelFrame(tab_genres, text="Befintlig genre i filens metadata (ffprobe)")
        frm_exist.pack(fill="x", pady=6)
        self.var_existing = tk.StringVar(value="")
        ttk.Entry(frm_exist, textvariable=self.var_existing, state="readonly").pack(fill="x", pady=3)
        ttk.Label(frm_exist, text="Du kan skriva över/ta bort genom att välja genrer och spara (tomt = rensa).").pack(anchor="w")

        frm_lists = ttk.Frame(tab_genres)
        frm_lists.pack(fill="both", expand=True, pady=6)
        frm_lists.columnconfigure(0, weight=1)
        frm_lists.columnconfigure(1, weight=1)
        frm_lists.rowconfigure(1, weight=1)

        ttk.Label(frm_lists, text="Video-genrer").grid(row=0, column=0, sticky="w")
        ttk.Label(frm_lists, text="Ljud-genrer").grid(row=0, column=1, sticky="w")

        self.video_genre_listbox = tk.Listbox(frm_lists, selectmode="multiple", height=12, exportselection=False)
        self.audio_genre_listbox = tk.Listbox(frm_lists, selectmode="multiple", height=12, exportselection=False)
        self.video_genre_listbox.grid(row=1, column=0, sticky="nsew", padx=(0, 8))
        self.audio_genre_listbox.grid(row=1, column=1, sticky="nsew")

        self._refresh_genre_listboxes()

        frm_gbtn = ttk.Frame(tab_genres)
        frm_gbtn.pack(fill="x", pady=6)
        ttk.Button(frm_gbtn, text="Applicera valda genrer till markerade filer", command=self.on_apply_genres_to_selected).pack(side="left")
        ttk.Button(frm_gbtn, text="Rensa genrer på markerade (sätt tomt)", command=self.on_clear_genres_selected).pack(side="left", padx=8)

        frm_manage = ttk.LabelFrame(tab_genres, text="Hantera genrelistor")
        frm_manage.pack(fill="x", pady=8)
        manage_row = ttk.Frame(frm_manage)
        manage_row.pack(fill="x", pady=4)

        ttk.Button(manage_row, text="Lägg till video-genre…", command=lambda: self.on_add_genre("video")).pack(side="left")
        ttk.Button(manage_row, text="Ta bort markerade video-genrer", command=lambda: self.on_remove_genre("video")).pack(side="left", padx=6)
        ttk.Button(manage_row, text="Lägg till ljud-genre…", command=lambda: self.on_add_genre("audio")).pack(side="left", padx=(18, 0))
        ttk.Button(manage_row, text="Ta bort markerade ljud-genrer", command=lambda: self.on_remove_genre("audio")).pack(side="left", padx=6)

        # ---- Tags tab ----
        ttk.Label(tab_tags, text="Taggar för ljud (MP3 + M4A)", font=("Helvetica", 12, "bold")).pack(anchor="w")
        self.lbl_tags = ttk.Label(tab_tags, text="Markera en .mp3 eller .m4a för att läsa taggar.")
        self.lbl_tags.pack(anchor="w", pady=(6, 8))

        frm_tags = ttk.LabelFrame(tab_tags, text="Fält (tomt = ta bort fält vid sparning)")
        frm_tags.pack(fill="x")

        self.tag_title = tk.StringVar(value="")
        self.tag_artist = tk.StringVar(value="")
        self.tag_album = tk.StringVar(value="")
        self.tag_track = tk.StringVar(value="")
        self.tag_year = tk.StringVar(value="")
        self.tag_genre = tk.StringVar(value="")
        self.tag_comment = tk.StringVar(value="")

        def add_row(parent, label, var):
            row = ttk.Frame(parent)
            row.pack(fill="x", pady=3)
            ttk.Label(row, text=label, width=10).pack(side="left")
            ttk.Entry(row, textvariable=var).pack(side="left", fill="x", expand=True)

        add_row(frm_tags, "Titel", self.tag_title)
        add_row(frm_tags, "Artist", self.tag_artist)
        add_row(frm_tags, "Album", self.tag_album)
        add_row(frm_tags, "Spår", self.tag_track)
        add_row(frm_tags, "År", self.tag_year)
        add_row(frm_tags, "Genre", self.tag_genre)
        add_row(frm_tags, "Kommentar", self.tag_comment)

        frm_cover = ttk.LabelFrame(tab_tags, text="Skivomslag (JPG/PNG)")
        frm_cover.pack(fill="both", expand=True, pady=10)

        cover_top = ttk.Frame(frm_cover)
        cover_top.pack(fill="x")
        ttk.Button(cover_top, text="Välj omslag…", command=self.on_cover_pick).pack(side="left")
        ttk.Button(cover_top, text="Ta bort omslag", command=self.on_cover_remove).pack(side="left", padx=8)
        ttk.Button(cover_top, text="Rensa formulär", command=self.on_tags_clear_form).pack(side="right")

        self.cover_hint = tk.StringVar(value="Förhandsvisning: (ingen)")
        ttk.Label(frm_cover, textvariable=self.cover_hint).pack(anchor="w", pady=(6, 6))
        self.cover_canvas = tk.Label(frm_cover, relief="groove")
        self.cover_canvas.pack(fill="both", expand=True)

        frm_tbtn = ttk.Frame(tab_tags)
        frm_tbtn.pack(fill="x", pady=10)
        ttk.Button(frm_tbtn, text="Läs taggar + omslag från markerad fil", command=self.on_tags_read_selected).pack(side="left")
        ttk.Button(frm_tbtn, text="Skriv taggar/omslag till markerade (.mp3/.m4a)", command=self.on_tags_write_selected).pack(side="left", padx=8)
        ttk.Button(frm_tbtn, text="Radera ALLA taggar på markerade (.mp3/.m4a)", command=self.on_tags_delete_selected).pack(side="left", padx=8)

        # ---- Save tab ----
        ttk.Label(tab_save, text="Spara ändringar", font=("Helvetica", 12, "bold")).pack(anchor="w")

        frm_mode = ttk.LabelFrame(tab_save, text="Läge")
        frm_mode.pack(fill="x", pady=8)

        self.var_save_mode = tk.StringVar(value="inplace")
        ttk.Radiobutton(frm_mode, text="Spara på befintliga filer (byt namn + skriv genre-tag)", value="inplace", variable=self.var_save_mode).pack(anchor="w")
        ttk.Radiobutton(frm_mode, text="Spara som nya filer (export)", value="export", variable=self.var_save_mode).pack(anchor="w")

        frm_export = ttk.LabelFrame(tab_save, text="Export-inställningar (om Spara som nya)")
        frm_export.pack(fill="x", pady=8)

        row = ttk.Frame(frm_export)
        row.pack(fill="x", pady=4)
        ttk.Label(row, text="Videoformat:").pack(side="left")
        self.var_video_fmt = tk.StringVar(value="mp4")
        ttk.Combobox(row, textvariable=self.var_video_fmt, values=EXPORT_VIDEO_FORMATS, width=8, state="readonly").pack(side="left", padx=6)

        ttk.Label(row, text="Ljudformat:").pack(side="left", padx=(18, 0))
        self.var_audio_fmt = tk.StringVar(value="m4a")
        ttk.Combobox(row, textvariable=self.var_audio_fmt, values=EXPORT_AUDIO_FORMATS, width=8, state="readonly").pack(side="left", padx=6)

        row2 = ttk.Frame(frm_export)
        row2.pack(fill="x", pady=4)
        ttk.Label(row2, text="Export-mapp:").pack(side="left")
        self.var_export_folder = tk.StringVar(value=str(Path.home() / "Desktop"))
        ttk.Entry(row2, textvariable=self.var_export_folder).pack(side="left", fill="x", expand=True, padx=6)
        ttk.Button(row2, text="Välj…", command=self.on_choose_export_folder).pack(side="left")

        frm_run = ttk.Frame(tab_save)
        frm_run.pack(fill="x", pady=10)
        ttk.Button(frm_run, text="Kör (på markerade filer)", command=self.on_run_save).pack(side="left")

        self.var_status = tk.StringVar(value="Redo")
        ttk.Label(tab_save, textvariable=self.var_status).pack(anchor="w")

        self.progress = ttk.Progressbar(tab_save, orient="horizontal", mode="determinate", maximum=100)
        self.progress.pack(fill="x", pady=6)

        self.txt_log = tk.Text(tab_save, height=14, wrap="word")
        self.txt_log.pack(fill="both", expand=True)

    def _update_tool_status_ui(self):
        s1 = "ffmpeg: OK" if self.ffmpeg_ok else "ffmpeg: saknas"
        s2 = "ffprobe: OK" if self.ffprobe_ok else "ffprobe: saknas"
        s3 = "Taggar: OK" if MUTAGEN_OK else "Taggar: saknas"
        s4 = "Pillow: OK" if PIL_OK else "Pillow: saknas (JPG preview)"
        self.var_tools.set(f"{s1} | {s2} | {s3} | {s4}")

    # ---------------- Folder loading ----------------
    def on_choose_folder(self):
        folder = filedialog.askdirectory(title="Välj mapp")
        if folder:
            self.load_folder(Path(folder))

    def on_refresh(self):
        if self.current_folder and self.current_folder.exists():
            self.load_folder(self.current_folder)

    def load_folder(self, folder: Path):
        self.current_folder = folder
        self.lbl_folder.config(text=str(folder))
        self.cfg["last_folder"] = str(folder)
        save_config(self.config_path, self.cfg)

        for iid in self.tree.get_children():
            self.tree.delete(iid)
        self.items.clear()
        self.item_by_iid.clear()
        self.iid_by_path.clear()

        self.var_existing.set("")
        self.lbl_sel.config(text="Markerad fil: (ingen)")
        self.lbl_tags.config(text="Markera en .mp3 eller .m4a för att läsa taggar.")
        self.on_tags_clear_form()
        self._clear_cover_preview()

        files = []
        try:
            for p in sorted(folder.iterdir()):
                if not p.is_file():
                    continue
                ext = p.suffix.lower()
                if ext in VIDEO_EXTS or ext in AUDIO_EXTS:
                    files.append(p)
        except Exception as e:
            messagebox.showerror("Fel", f"Kunde inte läsa mappen:\n{e}")
            return

        def worker():
            for p in files:
                ext = p.suffix.lower()
                kind = "video" if ext in VIDEO_EXTS else "audio"
                try:
                    size_b = p.stat().st_size
                except Exception:
                    size_b = -1

                md = extract_metadata(self.ffprobe_path if self.ffprobe_ok else None, str(p))
                existing = md.get("genre")

                item = FileItem(
                    path=p,
                    kind=kind,
                    size_bytes=size_b,
                    duration=md.get("duration"),
                    width=md.get("width"),
                    height=md.get("height"),
                    existing_genre=existing,
                    selected_genres=set(self._parse_genre_string(existing)),
                    new_name_stem=None
                )
                self.work_q.put(("add_item", item))
            self.work_q.put(("scan_done", None))

        threading.Thread(target=worker, daemon=True).start()
        self._log(f"Skannar: {folder}")

    def _parse_genre_string(self, s: str | None) -> list[str]:
        if not s:
            return []
        parts = re.split(r"[;,/]+", s)
        return [p.strip() for p in parts if p.strip()]

    def _genre_string(self, genres: set[str]) -> str:
        return "; ".join(sorted(genres))

    # ---------------- Tree interactions ----------------
    def on_tree_select(self, event=None):
        sel = self.tree.selection()
        if len(sel) == 1:
            item = self.item_by_iid.get(sel[0])
            if item:
                self.lbl_sel.config(text=f"Markerad fil: {item.path.name}")
                self.var_existing.set(item.existing_genre or "")
                self._sync_genre_selection_for_item(item)

                ext = item.path.suffix.lower()
                if item.kind == "audio" and ext in (".mp3", ".m4a"):
                    self.lbl_tags.config(text=f"Taggar: {item.path.name}")
                    if MUTAGEN_OK:
                        d = tags_read_mp3(item.path) if ext == ".mp3" else tags_read_m4a(item.path)
                        if d.get("has_cover") and d.get("cover_bytes"):
                            self._set_cover_preview_bytes(d["cover_bytes"], d.get("cover_mime") or "image/jpeg")
                        else:
                            self._clear_cover_preview()
                else:
                    self.lbl_tags.config(text="Taggar: (markera en .mp3 eller .m4a)")
                    self._clear_cover_preview()
        else:
            self.lbl_sel.config(text=f"Markerade filer: {len(sel)} st")
            self.var_existing.set("")
            self.lbl_tags.config(text="Taggar: (markera en .mp3 eller .m4a)")
            self._clear_cover_preview()

    def on_double_click(self, event=None):
        sel = self.tree.selection()
        if len(sel) != 1:
            return
        item = self.item_by_iid.get(sel[0])
        if not item:
            return
        new = simpledialog.askstring("Nytt namn", "Skriv nytt filnamn (utan filändelse):", initialvalue=item.path.stem)
        if new is None:
            return
        new = safe_filename(new)
        if not new:
            return
        item.new_name_stem = new
        self._refresh_row_for_item(sel[0], item)

    # ---------------- Rename logic ----------------
    def _compute_new_stem(self, item: FileItem, index: int, total: int) -> str:
        stem = item.path.stem
        newname = safe_filename(self.var_newname.get())
        if newname:
            stem = f"{newname} {index+1:02d}" if total > 1 else newname

        words_raw = self.var_remove_words.get().strip()
        if words_raw:
            words = [w.strip() for w in words_raw.split(",") if w.strip()]
            stem = remove_words_anywhere(stem, words)

        case = self.var_case.get()
        if case == "title":
            stem = title_case_words(stem)
        elif case == "lower":
            stem = stem.lower()

        stem = safe_filename(stem)
        return stem or item.path.stem

    def on_preview_rename(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Markera en eller flera filer i listan först.")
            return
        lines = []
        for i, iid in enumerate(sel):
            it = self.item_by_iid.get(iid)
            if not it:
                continue
            new_stem = self._compute_new_stem(it, i, len(sel))
            lines.append(f"{it.path.name}  →  {new_stem}{it.path.suffix}")
        self.txt_preview.delete("1.0", "end")
        self.txt_preview.insert("1.0", "\n".join(lines))

    def on_apply_rename_to_items(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Markera filer först.")
            return
        for i, iid in enumerate(sel):
            it = self.item_by_iid.get(iid)
            if not it:
                continue
            it.new_name_stem = self._compute_new_stem(it, i, len(sel))
            self._refresh_row_for_item(iid, it)
        self._log(f"Applicerade namnändring i listan på {len(sel)} filer (inte sparat på disk än).")

    # ---------------- CSV rename ----------------
    def on_csv_rename(self):
        if not self.current_folder:
            messagebox.showinfo("Info", "Välj en mapp först.")
            return

        csv_path = filedialog.askopenfilename(
            title="Välj CSV med gamla/nya filnamn",
            filetypes=[("CSV", "*.csv"), ("Alla filer", "*.*")]
        )
        if not csv_path:
            return

        try:
            mapping = self._read_csv_mapping(Path(csv_path))
        except Exception as e:
            messagebox.showerror("Fel", f"Kunde inte läsa CSV:\n{e}")
            return

        if not mapping:
            messagebox.showinfo("Info", "Hittade inga rader i CSV (kräver kolumnerna 'old' och 'new').")
            return

        stem_to_item = {it.path.stem: it for it in self.items}

        matches = []
        for old_stem, new_stem in mapping.items():
            it = stem_to_item.get(old_stem)
            if it:
                matches.append((it, old_stem, new_stem))

        if not matches:
            messagebox.showinfo("Info", "Inga matchningar hittades mot filerna i den öppna mappen.")
            return

        applied = 0
        for it, old_stem, new_stem in matches:
            ok = messagebox.askyesno(
                "CSV-rename",
                f"Byta namn?\n\n{it.path.name}\n\nFrån: {old_stem}\nTill:  {new_stem}\n\n"
                f"(Detta sparas inte på disk förrän du trycker 'Kör' under Spara.)"
            )
            if not ok:
                continue
            it.new_name_stem = safe_filename(new_stem) or it.path.stem
            iid = self.iid_by_path.get(str(it.path))
            if iid:
                self._refresh_row_for_item(iid, it)
            applied += 1

        self._log(f"CSV-rename: {applied} namnförslag applicerade (av {len(matches)} matchningar).")

    def _read_csv_mapping(self, path: Path) -> dict:
        mapping = {}
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return {}
            fields = [x.strip().lower() for x in reader.fieldnames]

            def pick(name):
                for i, fn in enumerate(fields):
                    if fn == name:
                        return reader.fieldnames[i]
                return None

            old_key = pick("old") or pick("gamla") or pick("old filename") or pick("old_name")
            new_key = pick("new") or pick("nya") or pick("new filename") or pick("new_name")
            if not old_key or not new_key:
                raise ValueError("CSV måste ha kolumnerna 'old' och 'new' (rubriker).")

            for row in reader:
                oldv = (row.get(old_key) or "").strip()
                newv = (row.get(new_key) or "").strip()
                if not oldv or not newv:
                    continue
                old_stem = Path(oldv).stem
                new_stem = Path(newv).stem
                mapping[old_stem] = new_stem
        return mapping

    # ---------------- Genres ----------------
    def _refresh_genre_listboxes(self):
        self.video_genre_listbox.delete(0, "end")
        self.audio_genre_listbox.delete(0, "end")
        for g in self.cfg.get("video_genres", []):
            self.video_genre_listbox.insert("end", g)
        for g in self.cfg.get("audio_genres", []):
            self.audio_genre_listbox.insert("end", g)

    def _sync_genre_selection_for_item(self, item: FileItem):
        self.video_genre_listbox.selection_clear(0, "end")
        self.audio_genre_listbox.selection_clear(0, "end")
        if item.kind == "video":
            genres = self.cfg.get("video_genres", [])
            for idx, g in enumerate(genres):
                if g in item.selected_genres:
                    self.video_genre_listbox.selection_set(idx)
        else:
            genres = self.cfg.get("audio_genres", [])
            for idx, g in enumerate(genres):
                if g in item.selected_genres:
                    self.audio_genre_listbox.selection_set(idx)

    def on_apply_genres_to_selected(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Markera filer först.")
            return

        v_sel = [self.video_genre_listbox.get(i) for i in self.video_genre_listbox.curselection()]
        a_sel = [self.audio_genre_listbox.get(i) for i in self.audio_genre_listbox.curselection()]
        v_set = set(v_sel)
        a_set = set(a_sel)

        n = 0
        for iid in sel:
            it = self.item_by_iid.get(iid)
            if not it:
                continue
            it.selected_genres = set(v_set) if it.kind == "video" else set(a_set)
            self._refresh_row_for_item(iid, it)
            n += 1

        self._log(f"Applicerade genrer till {n} filer (per filtyp).")

    def on_clear_genres_selected(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Markera filer först.")
            return
        for iid in sel:
            it = self.item_by_iid.get(iid)
            if not it:
                continue
            it.selected_genres = set()
            self._refresh_row_for_item(iid, it)
        self._log(f"Rensade genrer på {len(sel)} filer.")

    def on_add_genre(self, kind: str):
        g = simpledialog.askstring("Lägg till genre", "Skriv genre:")
        if not g:
            return
        g = g.strip()
        if not g:
            return
        key = "video_genres" if kind == "video" else "audio_genres"
        lst = self.cfg.get(key, [])
        if g in lst:
            return
        lst.append(g)
        lst.sort()
        self.cfg[key] = lst
        save_config(self.config_path, self.cfg)
        self._refresh_genre_listboxes()

    def on_remove_genre(self, kind: str):
        key = "video_genres" if kind == "video" else "audio_genres"
        lb = self.video_genre_listbox if kind == "video" else self.audio_genre_listbox
        idxs = list(lb.curselection())
        if not idxs:
            return
        genres = self.cfg.get(key, [])
        for i in sorted(idxs, reverse=True):
            try:
                del genres[i]
            except Exception:
                pass
        self.cfg[key] = genres
        save_config(self.config_path, self.cfg)
        self._refresh_genre_listboxes()

    # ---------------- Taggar + omslag UI ----------------
    def on_tags_clear_form(self):
        self.tag_title.set("")
        self.tag_artist.set("")
        self.tag_album.set("")
        self.tag_track.set("")
        self.tag_year.set("")
        self.tag_genre.set("")
        self.tag_comment.set("")
        self.pending_cover_action = None

    def _get_selected_single_audio_for_tags(self) -> FileItem | None:
        sel = self.tree.selection()
        if len(sel) != 1:
            return None
        it = self.item_by_iid.get(sel[0])
        if not it or it.kind != "audio":
            return None
        if it.path.suffix.lower() not in (".mp3", ".m4a"):
            return None
        return it

    def on_tags_read_selected(self):
        if not MUTAGEN_OK:
            messagebox.showerror("Mutagen saknas", "Installera: python3 -m pip install mutagen")
            return
        it = self._get_selected_single_audio_for_tags()
        if not it:
            messagebox.showinfo("Info", "Markera exakt en ljudfil som är .mp3 eller .m4a.")
            return

        ext = it.path.suffix.lower()
        d = tags_read_mp3(it.path) if ext == ".mp3" else tags_read_m4a(it.path)

        self.tag_title.set(d.get("title", ""))
        self.tag_artist.set(d.get("artist", ""))
        self.tag_album.set(d.get("album", ""))
        self.tag_track.set(d.get("track", ""))
        self.tag_year.set(d.get("year", ""))
        self.tag_genre.set(d.get("genre", ""))
        self.tag_comment.set(d.get("comment", ""))

        if d.get("has_cover") and d.get("cover_bytes"):
            self._set_cover_preview_bytes(d["cover_bytes"], d.get("cover_mime") or "image/jpeg")
            self.pending_cover_action = None
        else:
            self._clear_cover_preview()
            self.pending_cover_action = None

        self._log(f"Läste taggar: {it.path.name}")

    def on_tags_write_selected(self):
        if not MUTAGEN_OK:
            messagebox.showerror("Mutagen saknas", "Installera: python3 -m pip install mutagen")
            return
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Markera en eller flera filer (.mp3/.m4a).")
            return

        fields = {
            "title": self.tag_title.get(),
            "artist": self.tag_artist.get(),
            "album": self.tag_album.get(),
            "track": self.tag_track.get(),
            "year": self.tag_year.get(),
            "genre": self.tag_genre.get(),
            "comment": self.tag_comment.get(),
        }
        cover_action = self.pending_cover_action  # None/set/remove

        self.progress["value"] = 0
        self.var_status.set("Skriver taggar…")
        self._log("Skriver taggar…")

        def worker():
            total = len(sel)
            done = 0
            for iid in sel:
                it = self.item_by_iid.get(iid)
                if not it or it.kind != "audio":
                    continue
                ext = it.path.suffix.lower()
                if ext not in (".mp3", ".m4a"):
                    continue

                if ext == ".mp3":
                    ok, msg = tags_write_mp3(it.path, fields, cover_action)
                else:
                    ok, msg = tags_write_m4a(it.path, fields, cover_action)

                if ok:
                    self.work_q.put(("log", f"[OK] Taggar skrev: {it.path.name}"))
                else:
                    self.work_q.put(("log", f"[FEL] Taggar: {it.path.name}: {msg}"))

                done += 1
                self.work_q.put(("overall_progress", (done, total)))

            self.work_q.put(("done", None))

        threading.Thread(target=worker, daemon=True).start()

    def on_tags_delete_selected(self):
        if not MUTAGEN_OK:
            messagebox.showerror("Mutagen saknas", "Installera: python3 -m pip install mutagen")
            return
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Markera en eller flera filer (.mp3/.m4a).")
            return

        if not messagebox.askokcancel(
            "Radera taggar",
            "Detta raderar ALLA taggar (inkl. omslag) på markerade mp3/m4a-filer.\n\nFortsätta?"
        ):
            return

        self.progress["value"] = 0
        self.var_status.set("Raderar taggar…")
        self._log("Raderar taggar…")

        def worker():
            total = len(sel)
            done = 0
            for iid in sel:
                it = self.item_by_iid.get(iid)
                if not it or it.kind != "audio":
                    continue
                ext = it.path.suffix.lower()
                if ext not in (".mp3", ".m4a"):
                    continue

                if ext == ".mp3":
                    ok, msg = tags_remove_all_mp3(it.path)
                else:
                    ok, msg = tags_remove_all_m4a(it.path)

                if ok:
                    self.work_q.put(("log", f"[OK] Taggar raderade: {it.path.name}"))
                else:
                    self.work_q.put(("log", f"[FEL] Radera taggar: {it.path.name}: {msg}"))

                done += 1
                self.work_q.put(("overall_progress", (done, total)))

            self.work_q.put(("done", None))

        threading.Thread(target=worker, daemon=True).start()

    def on_cover_pick(self):
        if not MUTAGEN_OK:
            messagebox.showerror("Mutagen saknas", "Installera: python3 -m pip install mutagen")
            return

        img_path = filedialog.askopenfilename(
            title="Välj skivomslag (JPG/PNG)",
            filetypes=[("Images", "*.jpg *.jpeg *.png"), ("Alla filer", "*.*")]
        )
        if not img_path:
            return

        p = Path(img_path)
        if p.suffix.lower() not in (".jpg", ".jpeg", ".png"):
            messagebox.showerror("Fel", "Välj en .jpg/.jpeg eller .png.")
            return

        b = read_bytes(p)
        mime = guess_mime_from_ext(p)

        self.pending_cover_action = {"action": "set", "bytes": b, "mime": mime}
        self._set_cover_preview_file(p)
        self._log(f"Omslag valt: {p.name} (sparas när du trycker 'Skriv taggar…')")

    def on_cover_remove(self):
        if not MUTAGEN_OK:
            messagebox.showerror("Mutagen saknas", "Installera: python3 -m pip install mutagen")
            return
        self.pending_cover_action = {"action": "remove"}
        self._clear_cover_preview()
        self.cover_hint.set("Förhandsvisning: (tas bort vid nästa 'Skriv taggar…')")
        self._log("Omslag markeras för borttagning (sparas vid nästa 'Skriv taggar…').")

    def _clear_cover_preview(self):
        self._cover_preview_imgtk = None
        self.cover_canvas.configure(image="", text="")
        self.cover_hint.set("Förhandsvisning: (ingen)")

    def _set_cover_preview_bytes(self, img_bytes: bytes, mime: str):
        if PIL_OK:
            try:
                import io
                im = Image.open(io.BytesIO(img_bytes))
                im.thumbnail((380, 380))
                self._cover_preview_imgtk = ImageTk.PhotoImage(im)
                self.cover_canvas.configure(image=self._cover_preview_imgtk, text="")
                self.cover_hint.set("Förhandsvisning: (från filens taggar)")
                return
            except Exception:
                pass

        if mime == "image/png":
            self.cover_canvas.configure(image="", text="PNG-omslag hittat.\nInstallera Pillow för preview:\npython3 -m pip install pillow")
            self.cover_hint.set("Förhandsvisning: (PNG – installera Pillow)")
        else:
            self.cover_canvas.configure(image="", text="JPG-omslag hittat.\nInstallera Pillow för preview:\npython3 -m pip install pillow")
            self.cover_hint.set("Förhandsvisning: (JPG – installera Pillow)")

    def _set_cover_preview_file(self, path: Path):
        if PIL_OK:
            try:
                im = Image.open(str(path))
                im.thumbnail((380, 380))
                self._cover_preview_imgtk = ImageTk.PhotoImage(im)
                self.cover_canvas.configure(image=self._cover_preview_imgtk, text="")
                self.cover_hint.set(f"Förhandsvisning: {path.name}")
                return
            except Exception:
                pass

        if path.suffix.lower() == ".png":
            try:
                self._cover_preview_imgtk = tk.PhotoImage(file=str(path))
                self.cover_canvas.configure(image=self._cover_preview_imgtk, text="")
                self.cover_hint.set(f"Förhandsvisning: {path.name}")
                return
            except Exception:
                pass

        self.cover_canvas.configure(image="", text="Kunde inte förhandsvisa.\nInstallera Pillow:\npython3 -m pip install pillow")
        self.cover_hint.set("Förhandsvisning: (installera Pillow)")

    # ---------------- Export folder ----------------
    def on_choose_export_folder(self):
        folder = filedialog.askdirectory(title="Välj export-mapp")
        if folder:
            self.var_export_folder.set(folder)

    # ---------------- Save/Run (rename + genre via ffmpeg) ----------------
    def on_run_save(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Markera filer först.")
            return

        mode = self.var_save_mode.get()

        if mode == "inplace":
            if not self.ffmpeg_ok:
                if not messagebox.askokcancel(
                    "ffmpeg saknas",
                    "ffmpeg saknas eller kan inte köras.\n\n"
                    "Du kan fortfarande byta filnamn, men genre-metadata kan inte sparas.\n\n"
                    "Vill du fortsätta med endast filnamn?"
                ):
                    return

        if mode == "export":
            if not self.ffmpeg_ok:
                messagebox.showerror("ffmpeg saknas", "Export kräver ffmpeg (tools/ffmpeg).")
                return
            out_dir = Path(self.var_export_folder.get()).expanduser()
            if not out_dir.exists():
                messagebox.showerror("Fel", "Export-mappen finns inte.")
                return

        self.progress["value"] = 0
        self.var_status.set("Startar…")
        self._log("Startar körning…")

        def worker():
            total = len(sel)
            done = 0

            for iid in sel:
                it = self.item_by_iid.get(iid)
                if not it:
                    continue

                target_stem = safe_filename(it.new_name_stem or it.path.stem)
                genre_str = self._genre_string(it.selected_genres)

                if mode == "inplace":
                    new_path = it.path.with_name(target_stem + it.path.suffix)
                    if new_path != it.path:
                        try:
                            if new_path.exists():
                                raise FileExistsError(f"Filen finns redan: {new_path.name}")
                            old_path_str = str(it.path)
                            it.path.rename(new_path)
                            it.path = new_path
                            if old_path_str in self.iid_by_path:
                                self.iid_by_path[str(new_path)] = self.iid_by_path.pop(old_path_str)
                        except Exception as e:
                            self.work_q.put(("log", f"[FEL] Rename: {it.path.name}: {e}"))
                            done += 1
                            self.work_q.put(("overall_progress", (done, total)))
                            continue

                    if self.ffmpeg_ok:
                        ok, msg = ffmpeg_write_genre_inplace(self.ffmpeg_path, str(it.path), genre_str, progress_cb=None)
                        if ok and self.ffprobe_ok:
                            md = extract_metadata(self.ffprobe_path, str(it.path))
                            it.existing_genre = md.get("genre")
                        if ok:
                            self.work_q.put(("log", f"[OK] Sparade: {it.path.name} (genre='{genre_str}')"))
                        else:
                            self.work_q.put(("log", f"[FEL] Genre: {it.path.name}: {msg}"))
                    else:
                        self.work_q.put(("log", f"[OK] Sparade filnamn: {it.path.name} (genre ej sparad)"))

                else:
                    out_dir = Path(self.var_export_folder.get()).expanduser()
                    fmt = (self.var_video_fmt.get().lower() if it.kind == "video" else self.var_audio_fmt.get().lower())
                    dst = out_dir / f"{target_stem}.{fmt}"

                    ok, msg = ffmpeg_export(self.ffmpeg_path, str(it.path), str(dst), it.kind, progress_cb=None)
                    if ok:
                        # skriv genre på exportfil
                        ok2, msg2 = ffmpeg_write_genre_inplace(self.ffmpeg_path, str(dst), genre_str, progress_cb=None)
                        if not ok2:
                            self.work_q.put(("log", f"[VARNING] Kunde inte skriva genre på exportfil: {dst.name}: {msg2}"))
                        self.work_q.put(("log", f"[OK] Export: {dst.name}"))
                    else:
                        self.work_q.put(("log", f"[FEL] Export: {it.path.name}: {msg}"))

                done += 1
                self.work_q.put(("overall_progress", (done, total)))

            self.work_q.put(("done", None))

        threading.Thread(target=worker, daemon=True).start()

    # ---------------- Queue/UI updates ----------------
    def _poll_queue(self):
        try:
            while True:
                msg, payload = self.work_q.get_nowait()
                if msg == "add_item":
                    self._add_item_to_tree(payload)
                elif msg == "scan_done":
                    self.var_status.set(f"Laddade {len(self.items)} filer")
                    self._log(f"Klar. {len(self.items)} filer.")
                elif msg == "log":
                    self._log(payload)
                elif msg == "overall_progress":
                    done, total = payload
                    self.var_status.set(f"Klar: {done}/{total}")
                    if total > 0:
                        self.progress["value"] = (done / total) * 100.0
                elif msg == "done":
                    self.var_status.set("Klar")
                    self.progress["value"] = 100
                self.work_q.task_done()
        except queue.Empty:
            pass
        self.after(80, self._poll_queue)

    def _add_item_to_tree(self, item: FileItem):
        self.items.append(item)
        dims = ""
        if item.kind == "video" and item.width and item.height:
            dims = f"{item.width}×{item.height}"
        genres = self._genre_string(item.selected_genres)

        iid = self.tree.insert(
            "", "end",
            values=(
                item.path.name,
                human_size(item.size_bytes),
                human_time(item.duration if self.ffprobe_ok else None),
                dims if self.ffprobe_ok else "",
                genres,
                str(item.path)
            )
        )
        self.item_by_iid[iid] = item
        self.iid_by_path[str(item.path)] = iid

    def _refresh_row_for_item(self, iid: str, item: FileItem):
        dims = ""
        if item.kind == "video" and item.width and item.height:
            dims = f"{item.width}×{item.height}"
        name = (item.new_name_stem or item.path.stem) + item.path.suffix
        genres = self._genre_string(item.selected_genres)

        self.tree.item(iid, values=(
            name,
            human_size(item.size_bytes),
            human_time(item.duration if self.ffprobe_ok else None),
            dims if self.ffprobe_ok else "",
            genres,
            str(item.path)
        ))

    def _log(self, s: str):
        ts = time.strftime("%H:%M:%S")
        self.txt_log.insert("end", f"[{ts}] {s}\n")
        self.txt_log.see("end")


def main():
    app = App()
    app.mainloop()

if __name__ == "__main__":
    main()
