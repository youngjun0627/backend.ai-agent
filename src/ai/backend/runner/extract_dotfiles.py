from pathlib import Path
import json

if __name__ == '__main__':
    with open('/home/config/dotfiles.json') as fr:
        dotfiles = json.loads(fr.read())

    work_dir = Path('/home/work')
    for dotfile in dotfiles:
        file_path = work_dir / dotfile['path']
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(dotfile['data'])

        tmp = Path(file_path)
        while tmp != work_dir:
            tmp.chmod(int(dotfile['perm'], 8))
            tmp = tmp.parent
