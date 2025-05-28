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
./rdb-split --name dump.rdb --split 10
```

Then it will generate 10 or more files in directory 'rdb_dir'.

TIPS: You can change rdb file name and split number.

# For engula valuesight

```bash
mkdir analysis_output
docker run -it --rm -v "$(pwd)/rdb_dir:/tmp/rdb_dir" -v "$(pwd)/analysis_output:/engula/analysis_output" registry.cn-guangzhou.aliyuncs.com/montplex/engula-valuesight --batch
```

# Other

This tool use lib [rdb](https://github.com/hdt3213/rdb)

rdb-split will skip 'stream', 'module' type.