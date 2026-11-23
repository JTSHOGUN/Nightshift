"""Create a consistent portable checkpoint without copying an open SQLite WAL file."""
import argparse, os, sqlite3
from pathlib import Path
from contextlib import closing

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('destination',type=Path)
    parser.add_argument('--source',type=Path,default=Path(os.getenv('DESK_DB',Path(__file__).resolve().parent/'runtime'/'desk.sqlite')))
    a=parser.parse_args();source=a.source.resolve();dest=a.destination.resolve()
    if not source.is_file():parser.error('Source database does not exist')
    if dest.exists():parser.error('Refusing to overwrite an existing checkpoint')
    dest.parent.mkdir(parents=True,exist_ok=True)
    try:
        with closing(sqlite3.connect(source.as_uri()+'?mode=ro',uri=True)) as src,closing(sqlite3.connect(dest)) as target:
            src.backup(target,pages=128,sleep=.05)
            if target.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise RuntimeError('Backup integrity check failed')
    except BaseException:dest.unlink(missing_ok=True);raise
    print('Verified ledger checkpoint:',dest)
if __name__=='__main__':main()
