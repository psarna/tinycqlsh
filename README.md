### tinycqlsh - minimalistic cqlsh with almost no features!

I recommend using rlwrap for shell-like history and better user experience.

Usage examples:

```bash
./tinycqlsh 0 9042 <<< "INSERT INTO t (id, v) VALUES (1, 'hello');"
```
```bash
./tinycqlsh 127.0.0.1 9042 < file\_with\_cql\_queries.txt
```
```bash
rlwrap ./tinycqlsh localhost 9042
```
