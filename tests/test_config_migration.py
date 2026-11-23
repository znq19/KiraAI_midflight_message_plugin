"""One-time config migration regression tests.

Run: python3 tests/test_config_migration.py [plugin_dir]
"""
import json
import sys
import tempfile
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import harness as H  # noqa: E402


def old_cfg(run_value, step_present=None):
    cfg = {
        "section_basic": {"enabled": True},
        "section_flow": {"flow_method_group": "any", "flow_method_dm": "any"},
        "section_stop": {},
        "section_scope": {},
        "section_limits": {"max_inject_per_run": run_value},
        "section_inject": {},
        "section_media": {},
        "section_overrides": {},
    }
    if step_present is not None:
        cfg["section_limits"]["max_inject_per_step"] = step_present
    return cfg


def build(path, cfg):
    mod = H.load_plugin(path)
    plugin = mod.MidflightMessagePlugin(H.FakeCtx(), cfg)
    return plugin, cfg


def case_migrates_all_old_values(path):
    for old in (0, 4, 5, 200):
        p, cfg = build(path, old_cfg(old))
        assert p.max_inject_per_run == 0, (old, p.max_inject_per_run)
        assert p.max_inject_per_step == 0, (old, p.max_inject_per_step)
        assert cfg["config_version"] == 2, (old, cfg)
        assert cfg["section_limits"]["max_inject_per_run"] == 0, (old, cfg)
        assert cfg["section_limits"]["max_inject_per_step"] == 0, (old, cfg)
    return {"旧值 0/4/5/200 全部归零": True, "step 保持 0 自动": True}


def case_respects_new_step(path):
    p, cfg = build(path, old_cfg(4, step_present=8))
    assert p.max_inject_per_step == 8, p.max_inject_per_step
    assert p.max_inject_per_run == 0, p.max_inject_per_run
    assert cfg["config_version"] == 2
    return {"用户先填的新 step 不被旧值覆盖": True}


def case_only_once(path):
    p, cfg = build(path, old_cfg(4))
    assert p.max_inject_per_run == 0
    # 迁移后用户手动把整轮上限设为 50，下次加载必须保留，不能再清零
    cfg["section_limits"]["max_inject_per_run"] = 50
    p2, cfg2 = build(path, cfg)
    assert p2.max_inject_per_run == 50, p2.max_inject_per_run
    assert cfg2["config_version"] == 2
    return {"迁移只执行一次": True, "迁移后用户值被保留": True}


def case_writes_config_atomically(path):
    import core  # test stub package
    old_utils = getattr(core, "utils", None)
    old_mod = sys.modules.get("core.utils.path_utils")
    old_pkg = sys.modules.get("core.utils")
    try:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg_path = root / "plugins" / "midflight_message_plugin.json"
            cfg_path.parent.mkdir(parents=True)
            old = old_cfg(4)
            cfg_path.write_text(json.dumps(old, ensure_ascii=False), encoding="utf-8")
            utils = types.ModuleType("core.utils")
            path_utils = types.ModuleType("core.utils.path_utils")
            path_utils.get_config_path = lambda: root
            utils.path_utils = path_utils
            sys.modules["core.utils"] = utils
            sys.modules["core.utils.path_utils"] = path_utils
            core.utils = utils
            build(path, old)
            saved = json.loads(cfg_path.read_text(encoding="utf-8"))
            assert saved["config_version"] == 2, saved
            assert saved["section_limits"]["max_inject_per_run"] == 0, saved
            assert saved["section_limits"]["max_inject_per_step"] == 0, saved
    finally:
        if old_pkg is not None:
            sys.modules["core.utils"] = old_pkg
        else:
            sys.modules.pop("core.utils", None)
        if old_mod is not None:
            sys.modules["core.utils.path_utils"] = old_mod
        else:
            sys.modules.pop("core.utils.path_utils", None)
        if old_utils is not None:
            core.utils = old_utils
        elif hasattr(core, "utils"):
            delattr(core, "utils")
    return {"迁移原子写回": True, "文件值为 v2/0/0": True}


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else str(HERE.parent)
    path = str(Path(path).parent) if Path(path).is_file() else str(Path(path))
    for case in (case_migrates_all_old_values, case_respects_new_step, case_only_once,
                 case_writes_config_atomically):
        print("  PASS", case(path))
    print("test_config_migration: ALL PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
