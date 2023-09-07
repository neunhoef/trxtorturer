#!/usr/bin/fish

function getMetricsValue
  set stage $argv[1]
  set metric $argv[2]
  set result (grep "^$metric" "$stage.metrics" | awk -e '{print $2}')
  echo $result
end

function run
  set nrtrx $argv[1]
  set keysize $argv[2]
  trxtorturer "$nrtrx" 15 1000 30 > /dev/null 2> /dev/null
  set resident_before (getMetricsValue start "arangodb_process_statistics_resident_set_size[{]")
  set resident_after (getMetricsValue largetrx "arangodb_process_statistics_resident_set_size[{]")
  set trxmetric (getMetricsValue largetrx "arangodb_transactions_rest")
  set rssdiff (math $resident_after - $resident_before)
  echo Trx: $nrtrx Resident increase: $rssdiff Trx metric report: $trxmetric Difference: (math $resident_after - $resident_before - $trxmetric) Fraction: (math $trxmetric / $rssdiff)
end

echo Small keys:

run 10 8
run 20 8
run 30 8
run 40 8
run 50 8
run 60 8
run 70 8
run 80 8
run 90 8
run 100 8

echo Large keys:

run 10 80
run 20 80
run 30 80
run 40 80
run 50 80
run 60 80
run 70 80
run 80 80
run 90 80
run 100 80

echo Extra large keys:

run 10 800
run 20 800
run 30 800
run 40 800
run 50 800
run 60 800
run 70 800
run 80 800
run 90 800
run 100 800
