# rdb-split

Split one RDB file to several RDB files.

# Usage

## Prepare

- Go 1.24

## Build

```bash
git clone https://github.com/montplex/rdb-split.git
cd rdb-split
go build
```

## Run

Copy or move your RDB file to the same directory as rdb-split.

```bash
mv <your-rdb-file> .
```

Run rdb-split.

```bash
./rdb-split --name dump.rdb --target-size-gb 5
```

It means split dump.rdb to several files, each file size is less than 5GB as rdb encode do compress.

Then it will generate several files in directory 'rdb_dir'.

TIPS: You can change rdb file name --name or --target-size-gb.

# For engula valuesight

```bash
mkdir analysis_output
docker run -it --rm -v "$(pwd)/rdb_dir:/tmp/rdb_dir" -v "$(pwd)/analysis_output:/engula/analysis_output" registry.cn-guangzhou.aliyuncs.com/montplex/engula-valuesight --batch
```

# Other

This tool use lib [rdb](https://github.com/hdt3213/rdb)

rdb-split will skip 'stream', 'module' type.