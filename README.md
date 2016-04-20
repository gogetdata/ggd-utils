# ggd-utils

cmd/check-sort-order/ takes a genome file and (currently) a .vcf.gz or a .bed.gz and

checks that:

+ a .tbi is present
+ the VCF has ""##fileformat=VCF" as the first line
+ the VCF has a #CHROM header
+ the chromosome are in the order specified by the genome file (and present)
+ the positions are sorted
+ the positions are <= the chromosome lengths defined in the genome file.

As a result, any new genome going into GGD will have a .genome file that will dictate
the sort order and presence or absence of the 'chr' prefix for chromosomes.


<!--


for arch in 386 amd64; do
    for os in darwin linux; do
		rm -rf $os/$arch/
		mkdir -p $os/$arch/
        GOOS=$os GOARCH=$arch go build -o ${os}/${arch}/check-sort-order cmd/check-sort-order/main.go
    done
done

-->
