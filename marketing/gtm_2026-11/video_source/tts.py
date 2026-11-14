"""Narrate each slide's speaker notes with Gemini TTS -> audio/<id>.wav.
Usage: python3 tts.py <deck_root> <out_dir> [slide_id ...]"""
import base64, json, os, re, sys, time, urllib.request, wave, html

deck_root, out_dir = sys.argv[1], sys.argv[2]
only = sys.argv[3:]
key = os.environ["GEMINI_API_KEY"]
MODEL = os.environ.get("TTS_MODEL", "gemini-3.8-flash-tts")
VOICE = os.environ.get("TTS_VOICE", "Kore")
STYLE = "Say in a warm, friendly explainer voice: "

deck = json.load(open(os.path.join(deck_root, "project/deck.json")))
os.makedirs(out_dir, exist_ok=True)
for sid in deck["order"]:
    if only and sid not in only:
        continue
    src = open(os.path.join(deck_root, f"project/slides/{sid}.html"), encoding="utf-8").read()
    notes = html.unescape(re.search(r"<aside>(.*?)</aside>", src, re.S).group(1)).strip()
    notes = notes.replace("SchoolNossa", "School Nossa")
    body = {"contents": [{"parts": [{"text": notes}]}],
            "generationConfig": {"responseModalities": ["AUDIO"],
                                 "speechConfig": {"voiceConfig": {"prebuiltVoiceConfig": {"voiceName": VOICE}}}}}
    req = urllib.request.Request(
        f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent",
        data=json.dumps(body).encode(), headers={"Content-Type": "application/json", "x-goog-api-key": key,
                                                 "User-Agent": "schoolnossa-video/1.0"})
    for attempt in range(4):
        try:
            resp = json.load(urllib.request.urlopen(req, timeout=180))
            break
        except Exception as e:  # retry transient errors
            print(sid, "retry", attempt, e, file=sys.stderr)
            time.sleep(5 * (attempt + 1))
    else:
        sys.exit(f"TTS failed for {sid}")
    part = resp["candidates"][0]["content"]["parts"][0]["inlineData"]
    pcm = base64.b64decode(part["data"])
    mt = part.get("mimeType", "")
    m = re.search(r"rate=(\d+)", mt)
    rate = int(m.group(1)) if m else 24000
    if "wav" in mt or pcm[:4] == b"RIFF":
        open(os.path.join(out_dir, f"{sid}.wav"), "wb").write(pcm)
        print(sid, "wav as-is", mt); continue
    path = os.path.join(out_dir, f"{sid}.wav")
    with wave.open(path, "wb") as w:
        w.setnchannels(1); w.setsampwidth(2); w.setframerate(rate); w.writeframes(pcm)
    print(sid, f"{len(pcm) / 2 / rate:.1f}s", part.get("mimeType"))
