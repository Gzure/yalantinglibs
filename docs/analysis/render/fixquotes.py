import re, pathlib
p = pathlib.Path('docs/analysis/io_context_executor_report.md')
s = p.read_text(encoding='utf-8')
s2 = re.sub(r'(participant\s+\w+\s+as\s+)"([^"\n]+)"', r'\1\2', s)
s2 = re.sub(r'(actor\s+\w+\s+as\s+)"([^"\n]+)"', r'\1\2', s2)
p.write_text(s2, encoding='utf-8')
print('quoted participants left:', len(re.findall(r'participant\s+\w+\s+as\s+"', s2)))
# re-extract mermaid blocks
blocks = re.findall(r'```mermaid\n(.*?)```', s2, re.S)
out = pathlib.Path('docs/analysis/render')
for i, b in enumerate(blocks, 1):
    (out / f'diagram{i:02d}.mmd').write_text(b.strip() + '\n', encoding='utf-8')
print(len(blocks), 'blocks re-extracted')
