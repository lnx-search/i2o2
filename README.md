# I2o2

Schedule and execute IO calls an io_uring executor.

This project is designed for [lnx](https://github.com/lnx-search/lnx) as a replacement to glommio's file system API
and is a relatively low-level API.

In particular, the system only lightly attempts to prevent you messing stuff up, hence why almost all calls
are unsafe. The only thing it really protects against buffers being dropped early.