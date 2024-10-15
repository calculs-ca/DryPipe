rm -Rf d

mkdir d
mkdir -p d/a1/x/d/r
mkdir -p d/a2
mkdir -p d/a3

touch d/a1/a.yes
touch d/a1/x/d/r/a.yes

touch d/a1/f.no
touch d/a2/f.no
touch d/a2/f.nono
touch d/a3/abc.nono


cat <<EOT >> d/a1/.rsync-filter
- /**.no
+ /**
EOT

cat <<EOT >> fi
: a1/.rsync-filter
+ a1/
- /**
EOT


rsync --dry-run -i -a --include-from=fi d/ /tmp