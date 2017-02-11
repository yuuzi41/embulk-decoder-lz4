# Lz4 decoder plugin for Embulk

This plugin is embulk file decoder for LZ4, a Extremely fast compression algorithm.

**Caution!** : This plugin supports [LZ4 Frame Format](https://github.com/lz4/lz4/wiki/lz4_Frame_format.md) only!

_LZ4 Frame Format_ is the most famously format for compressing file of any size by LZ4. if you use Linux, `lz4` program has generating a compressed file formatted by _LZ4 Frame Format_.

then, this plugin is not supported other formats based on LZ4 algorithm such as raw _LZ4 Block Format_, currently.

## Overview

* **Plugin type**: decoder
* **Guess supported**: no

## Configuration

No configurable.

## Example

```yaml
in:
  type: any output input plugin type
  decoders:
    - {type: lz4}
```


```
$ embulk gem install embulk-decoder-lz4
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
