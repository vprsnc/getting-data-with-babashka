#!/usr/bin/env sh

cd $ETL_DIR &&
    printf "Starting ETL process  $(date) \n\n" >> binance.log &&
    bb $ETL_DIR/binance-download.clj >> binance.log &&
    bb $ETL_DIR/binance-edn-parser.clj >> binance.log &&
    bb $ETL_DIR/binance-to-db.clj >> binance.log &&
    printf " \n\n ETL process ended on  $(date) \n\n" >> binance.log
