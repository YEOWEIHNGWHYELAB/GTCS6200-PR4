# Setup

## Command

Windows Mount:

```Shell
docker run --name cs6200-pr4 -it ^
  -v "%cd%\part1:/cs6200/part1" ^
  -v "%cd%\part2:/cs6200/part2" ^
  -v "%cd%\bin:/cs6200/bin" ^
  -v "%cd%\mnt:/cs6200/mnt" ^
  -v "%cd%\tmp:/cs6200/tmp" ^
  gtcs6200-env
```

Start container: `docker start -ai cs6200-pr4`

Open Running Container: `docker exec -it cs6200-pr4 /bin/bash`
