import json, pathlib
cfg = {"executablePath": r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
       "args": ["--no-sandbox", "--disable-gpu"]}
pathlib.Path('docs/analysis/render/puppeteer.json').write_text(json.dumps(cfg, ensure_ascii=False), encoding='utf-8')
print(pathlib.Path('docs/analysis/render/puppeteer.json').read_text(encoding='utf-8'))
