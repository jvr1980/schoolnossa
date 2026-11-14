"""Transcribe each narration WAV with Gemini and compare it with the slide notes.
Flags a slide when the spoken text starts with the style prompt or differs a lot.
Usage: python3 check_audio.py <deck_root> <audio_dir> [slide_id ...]"""
import base64, difflib, html, json, os, re, sys, urllib.request

deck_root, audio_dir = sys.argv[1], sys.argv[2]
only = sys.argv[3:]
key = os.environ["GEMINI_API_KEY"]
deck = json.load(open(os.path.join(deck_root, "project/deck.json")))

def words(t):
    return re.findall(r"[a-z0-9]+", t.lower().replace("schoolnossa", "school nossa"))

bad = 0
for sid in deck["order"]:
    if only and sid not in only:
        continue
    src = open(os.path.join(deck_root, f"project/slides/{sid}.html"), encoding="utf-8").read()
    notes = html.unescape(re.search(r"<aside>(.*?)</aside>", src, re.S).group(1)).strip()
    b = base64.b64encode(open(os.path.join(audio_dir, f"{sid}.wav"), "rb").read()).decode()
    body = {"contents": [{"parts": [{"text": "Transcribe this audio verbatim. Output only the transcript."},
                                    {"inlineData": {"mimeType": "audio/wav", "data": b}}]}]}
    req = urllib.request.Request(
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.8-flash:generateContent",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-goog-api-key": key, "User-Agent": "schoolnossa-video/1.0"})
    text = json.load(urllib.request.urlopen(req, timeout=180))["candidates"][0]["content"]["parts"][0]["text"]
    ratio = difflib.SequenceMatcher(None, words(notes), words(text)).ratio()
    leak = any(w in text.lower()[:80] for w in ("say in", "explainer voice", "read this"))
    flag = "BAD" if leak or ratio < 0.85 else "ok"
    bad += flag == "BAD"
    print(f"{flag} {sid} match={ratio:.2f} start={text[:70]!r}")
sys.exit(1 if bad else 0)
