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


#rsync --dry-run -i -a -f '+ a1/' -f '- a1/*.no' -f '+ a1/**' -f '- /**' d/ /tmp

#rsync --dry-run  -i -a -f '+ a1/*' -f '- a1/*.no' -f '+ a1/**' -f '- /**' d/ /tmp

rsync --dry-run -i -a -f '+ /a1/' -f '+ /a1/**' -f '- *.no' -f '+ a1/**/*.yes'  -f '- /**' d/ /tmp