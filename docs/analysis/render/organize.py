import pathlib, shutil, re
src = pathlib.Path('docs/analysis/render')
img = pathlib.Path('docs/analysis/img')
img.mkdir(parents=True, exist_ok=True)
names = {
 1: 'fig01_architecture', 2: 'fig02_post_dispatch_defer', 3: 'fig03_do_run_one_loop',
 4: 'fig04_reactor_task', 5: 'fig05_wakeup_paths', 6: 'fig06_work_count_state',
 7: 'fig07_io_completion', 8: 'fig08_schedule_hint_branch', 9: 'fig09_pool_structure',
 10: 'fig10_round_robin', 11: 'fig11_pool_lifecycle', 12: 'fig12_stop_chain',
 13: 'fig13_coro_post_resume',
}
for n, name in names.items():
    s = src / f'diagram{n:02d}.png'
    if s.exists():
        shutil.copyfile(s, img / f'{name}.png')
print([p.name for p in sorted(img.glob('*.png'))])

# 在报告末尾追加图索引
p = pathlib.Path('docs/analysis/io_context_executor_report.md')
s = p.read_text(encoding='utf-8')
titles = {
 1: '分层架构总图（§1）',
 2: 'post / dispatch / defer 到入队的路径（§3.1）',
 3: 'do_run_one 调度主循环（§4.4，核心）',
 4: 'task_operation_ 与 reactor 的协作（§4.5）',
 5: '两条唤醒路径（§4.6）',
 6: 'work 计数生命周期（§4.7）',
 7: 'IO 完成路径 epoll → handler（§4.8）',
 8: 'ExecutorWrapper::schedule(hint) 分支（§5.1）',
 9: 'io_context_pool 内部结构（§5.2）',
 10: 'round-robin 分配与 coro_rpc 取用（§5.3）',
 11: 'io_context_pool 生命周期（§5.4）',
 12: 'stop() 关闭链条（§5.4）',
 13: 'co_await coro_io::post 的协程恢复链路（§6.1）',
}
lines = ['', '---', '', '## 附录二：渲染后的图（PNG）', '', '| 文件 | 内容 |', '|---|---|']
for n, name in names.items():
    f = img / f'{name}.png'
    if f.exists():
        lines.append(f'| `docs/analysis/img/{name}.png` | {titles[n]} |')
s = s.rstrip('\n') + '\n' + '\n'.join(lines) + '\n'
p.write_text(s, encoding='utf-8')
print('appended index, report chars =', len(s))
