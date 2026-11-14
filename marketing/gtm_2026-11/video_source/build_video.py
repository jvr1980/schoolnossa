"""Assemble slide PNGs + narration WAVs into one narrated MP4 with crossfades and soft subtitles.
Usage: python3 build_video.py <deck_root> <png_dir> <audio_dir> <out.mp4>"""
import html, json, os, re, subprocess, sys

deck_root, png_dir, audio_dir, out = sys.argv[1:5]
LEAD, TAIL, XF = 0.6, 0.9, 0.6  # silence before/after each narration, crossfade length (s)
order = json.load(open(os.path.join(deck_root, "project/deck.json")))["order"]

def dur(p):
    return float(subprocess.check_output(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                                          "-of", "csv=p=0", p]).decode())

a = [dur(os.path.join(audio_dir, f"{s}.wav")) for s in order]
d = [x + LEAD + TAIL for x in a]
offsets, t = [], 0.0  # start time of each slide in the final timeline
for i, x in enumerate(d):
    offsets.append(t)
    t += x - XF
total = offsets[-1] + d[-1]

cmd = ["ffmpeg", "-v", "error", "-y"]
for s, x in zip(order, d):
    cmd += ["-loop", "1", "-framerate", "30", "-t", f"{x:.3f}", "-i", os.path.join(png_dir, f"{s}.png")]
for s in order:
    cmd += ["-i", os.path.join(audio_dir, f"{s}.wav")]
n = len(order)
f = []
for i in range(n):
    f.append(f"[{i}:v]format=yuv420p,setsar=1[v{i}]")
prev = "v0"
for i in range(1, n):
    off = offsets[i]
    f.append(f"[{prev}][v{i}]xfade=transition=fade:duration={XF}:offset={off:.3f}[x{i}]")
    prev = f"x{i}"
for i in range(n):
    ms = int((offsets[i] + LEAD) * 1000)
    f.append(f"[{n + i}:a]aresample=48000,adelay={ms}:all=1[a{i}]")
f.append("".join(f"[a{i}]" for i in range(n)) + f"amix=inputs={n}:normalize=0,apad=whole_dur={total:.3f}[aout]")

# Soft subtitles: split each slide's notes into sentences, time them by length within the narration.
def ts(x):
    h, m = int(x // 3600), int(x % 3600 // 60)
    return f"{h:02d}:{m:02d}:{x % 60:06.3f}".replace(".", ",")
srt, k = [], 1
for i, s in enumerate(order):
    src = open(os.path.join(deck_root, f"project/slides/{s}.html"), encoding="utf-8").read()
    notes = html.unescape(re.search(r"<aside>(.*?)</aside>", src, re.S).group(1)).strip()
    sents = [x.strip() for x in re.split(r"(?<=[.!?])\s+", notes) if x.strip()]
    chars = sum(len(x) for x in sents)
    cur = offsets[i] + LEAD
    for x in sents:
        span = a[i] * len(x) / chars
        srt.append(f"{k}\n{ts(cur)} --> {ts(cur + span - 0.05)}\n{x}\n")
        cur += span
        k += 1
srt_path = os.path.splitext(out)[0] + ".srt"
open(srt_path, "w", encoding="utf-8").write("\n".join(srt))

cmd += ["-i", srt_path, "-filter_complex", ";".join(f), "-map", f"[{prev}]", "-map", "[aout]", "-map", f"{2 * n}:s",
        "-c:v", "libx264", "-preset", "medium", "-crf", "20", "-pix_fmt", "yuv420p", "-r", "30",
        "-c:a", "aac", "-b:a", "160k", "-c:s", "mov_text", "-metadata:s:s:0", "language=eng",
        "-t", f"{total:.3f}", "-movflags", "+faststart", out]
subprocess.run(cmd, check=True)
print(f"{out}: {total:.1f}s, {n} slides; subtitles {srt_path}")
for s, o in zip(order, offsets):
    print(f"  {o:6.1f}s {s}")
