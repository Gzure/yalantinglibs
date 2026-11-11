import pathlib, re
p = pathlib.Path('docs/analysis/io_context_executor_report.md')
s = p.read_text(encoding='utf-8')
s = s.replace('Running --> Stopping: stop(force=true) 清 work_ 且 io_context::stop()',
              'Running --> Stopping: stop(force=true) 清 work_ 且 io_context stop()')
p.write_text(s, encoding='utf-8')
blocks = re.findall(r'```mermaid\n(.*?)```', s, re.S)
out = pathlib.Path('docs/analysis/render')
for i, b in enumerate(blocks, 1):
    (out / f'diagram{i:02d}.mmd').write_text(b.strip() + '\n', encoding='utf-8')
print('ok', len(blocks))
