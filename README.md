# PyCASC
(This product is very unfinished and should not be used for anything other than entertainment/simple exports. Report any exceptions thrown in issues)

A Python3 CASC library and pyqt CASC Viewer app.

This is part of a much larger year-long project, that will hopefully result in a very nice blizzard reverse-engineering toolkit.

## 공지사항
- 파이썬을 익히기 위한 용도도 있습니다.
- 대부분 문서화나 주석은 한글로 작성할 예정입니다.
- 원본 코드는 여기를 사용하세요 : [RaidAndFade/PyCASC](https://github.com/RaidAndFade/PyCASC).

## 개발환경
- Python 3.10.1
- VS Code 1.65.0
- 

## CASC? 
Blizzard's proprietary data storage format (used in practically all of their products)

## Why?
There exists so many instances of CASC viewing libs from generations ago, each with their own benefits / losses, but all in languages such as C or C#. And all depending on the same C++ based CASCLib.

Now there isn't anything wrong with that, but there also isn't anything wrong with a bit of expansion.
This is a PURE Python implementation of Casc, implementing a full-featured reader, as well as a sample PyQT5 usage of the library.

## How do I use it?
As a library? There is a plethora of undocumented api endpoints and ways to use the library. The main methods are outlined in PyCASC/__init__.py at the bottom of the file.

The library is currently undocumented because it is in early alpha, proper documentation will come as the library develops.

As an app? You modify CASCViewApp.py at the very bottom, and change it to the directory of Blizzard game. And then run that file (assuming you have all of the packages in requirements.txt installed, the CASCViewApp's requirements differ from the library).

## What's the library do?
- Open a CASC filesystem (Supports most CASC games)
- List all files that exist in both the filesystem and the rootfile
- Read individual files into memory (for exporting or analysis)

## What's the app do?
Current features:
- Explore the file-tree of a CASC filesystem
- Export individual files one at a time
- View files as hexdumps
- View text-files as text files
- Open files externally, without having to export them.
- View very basic file/folder information (basically just file size)
- Folder exports ( export an entire file tree, with folder structure )

Planned features:
- DBC viewing & exporting as sql/csv ( Blizzard proprietary database format)
- BLP viewing & exporting ( Blizzard proprietary image format )
