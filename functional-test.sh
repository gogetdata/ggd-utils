#!/bin/bash


test -e ssshtest || wget -q https://raw.githubusercontent.com/ryanlayer/ssshtest/master/ssshtest

. ssshtest

set -o nounset

go build -o ./check-sort-order cmd/check-sort-order/main.go 


## Check vcf file
run check_vcf_file ./check-sort-order --genome test_genome_files/genome_sizes.genome test_files/test_file.vcf.gz   
assert_exit_code 0
assert_no_stdout

## check bed file
run check_bed_file ./check-sort-order --genome test_genome_files/genome_sizes.genome test_files/test_file.bed.gz   
assert_exit_code 0
assert_no_stdout

## check gtf file 
run check_gtf_file ./check-sort-order --genome test_genome_files/genome_sizes.genome test_files/test_file.gtf.gz   
assert_exit_code 0
assert_no_stdout

## Check bad sorted bed 
run check_bad_sorted_bed_file ./check-sort-order --genome test_genome_files/genome_sizes.genome test_files/test_file.bad_sort.bed.gz  
assert_exit_code 1
assert_in_stderr "chromosomes not in specified sort order: 4, 1 at line 33"
assert_in_stderr "use gsort (https://github.com/brentp/gsort/) to order according to the genome file"

## check a vcf file with a bad header
run check_bad_sorted_bed_file ./check-sort-order --genome test_genome_files/genome_sizes.genome test_files/test_file.bad_header.vcf.gz
assert_exit_code 1
assert_in_stderr "VCF header line '##fileformat=VCF... not found"

## Check a tsv file that is formated like a vcf file
run check_bad_sorted_bed_file ./check-sort-order --genome test_genome_files/genome_sizes.genome test_files/test_bed_like.tsv.gz
assert_exit_code 0
assert_no_stdout

## Check a tsv file that is formated like a bed file
run check_bad_sorted_bed_file ./check-sort-order --genome test_genome_files/genome_sizes.genome test_files/test_bed_like.tsv.gz
assert_exit_code 0
assert_no_stdout

## Check a tsv file that is not formated correctly 
run check_bad_sorted_bed_file ./check-sort-order --genome test_genome_files/genome_sizes.genome test_files/test_bad.vcf_like.tsv.gz
assert_exit_code 1
assert_in_stderr 'strconv.Atoi: parsing "A": invalid syntax'


