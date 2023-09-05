# Trx torturer

This program opens 100 relatively large transactions in a single ArangoDB
instances, then synchronizes all 100 threads, before committing all
transactions. This is used to check memory usage during transactions.
