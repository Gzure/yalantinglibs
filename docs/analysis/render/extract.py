import re, pathlib
src = pathlib.Path('docs/analysis/io_context_executor_report.md').read_text(encoding='utf-8')
blocks = re.findall(r'```mermaid\n(.*?)```', src, re.S)
out = pathlib.Path('docs/analysis/render')
for i, b in enumerate(blocks, 1):
    (out / f'diagram{i:02d}.mmd').write_text(b.strip() + '\n', encoding='utf-8')
print(len(blocks), 'blocks extracted')
