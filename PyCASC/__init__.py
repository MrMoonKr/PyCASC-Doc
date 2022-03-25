from dataclasses import dataclass
import os
import struct
import pickle
from io import BytesIO
from typing import Union, Dict

import fnmatch

CACHE_DURATION = 3600
CACHE_DIRECTORY = os.path.join( os.getcwd(), "cache" )
# CACHE_DIRECTORY = "/Volumes/USB2/pycasccache/"
# CACHE_DIRECTORY = "/Volumes/Secure/pycasc"

LISTFILE = (os.path.join(os.getcwd(),"listfiles","wow-82.txt"),"82")

TACT_KEYS = {} # dict of name:key, populated automatically for some games.

from PyCASC.utils.blizzutils import var_int,have_cached,get_cdn_url,hashlittle2,parse_build_config,parse_config,prefix_hash,hexkey_to_bytes,byteskey_to_hex
from PyCASC.utils.CASCUtils import parse_encoding_file,parse_install_file,parse_download_file,parse_root_file,r_cascfile,cascfile_size, NAMED_FILE,SNO_FILE,SNO_INDEXED_FILE,WOW_HASHED_FILE,WOW_DATAID_FILE


def prep_6x_listfile(fp):
    names={}
    if os.path.exists(os.path.join(fp,".pkl")):
        return pickle.load(open(fp+".pkl",'rb'))
    with open(fp,"r") as f:
        for x in f.readlines():
            x=x.strip()
            x=x.upper()
            x=x.replace("/","\\")
            hsha,hshb = hashlittle2(x)
            names[hsha<<20 | hshb]=x
    pickle.dump(names, open(fp+".pkl",'wb+'))
    return names

def prep_82_listfile(fp):
    names={}
    with open(fp,"r") as f:
        for x in f.readlines():
            i,f = x.split(";",2)
            names[int(i)]=f.strip()
    return names


class FileInfo:
    """에셋정보저장클래스"""
    ekey:int
    ckey:int
    data_file:int
    offset:int
    compressed_size:int
    uncompressed_size:int
    chunk_count:int
    name:str
    extras:dict
    
def r_idx( file_name: str ) -> list[FileInfo]:
    """ /World of Warcraft/Data/data/~~~.idx 파일 로딩 """

    ents=[]
    with open( file_name, 'rb' ) as f:
        # 40( 0x28 )바이트 파일헤더 읽기
        (
            header_hash_size,
            header_hash,
            unknown_0,
            bucket_index,
            unknown_1,
            entry_size_bytes,
            entry_offset_bytes,
            entry_key_bytes,
            archive_file_header_bytes,
            archive_total_size_maximum,
            padding,
            entries_size,
            entries_hash
        ) = struct.unpack( "IIH6BQQII", f.read( 0x28 ) )

        esize = entry_size_bytes + entry_offset_bytes + entry_key_bytes
        for x in range( 0x28, 0x28 + entries_size, esize ):
            ek = var_int( f, entry_key_bytes, False )       # 9 bytes, key
            eo = var_int( f, entry_offset_bytes, False )    # 5 bytes, 40bits = 10bits ( archive index ) + 30bits ( offset )
            es = var_int( f, entry_size_bytes )             # 4 bytes, size

            e                   = FileInfo()
            e.data_file         = eo >> 30
            e.offset            = eo & ( 2**30-1 )
            e.compressed_size   = es
            e.ekey              = ek
            #print( e )
            ents.append( e )
    return ents

def r_cidx(df): 
    d = BytesIO(df)

    curchksz=0x10
    tocCHK, vrsn,u2,u1, bs, eos, ess, eks, chksz, numel, ftCHK = (None,)*11
    validFooter = False
    while not validFooter and curchksz>0:
        ftrsize = curchksz*2 + 12
        d.seek(-ftrsize,2)

        tocCHK = d.read(curchksz)
        vrsn,u2,u1,bs,eos,ess,eks,chksz,numel = struct.unpack(f"8bI", d.read(12))
        ftCHK = d.read(curchksz)

        # a valid footer has version 1 (that i know of), and the length of the CHKs == chksz.
        validFooter = len(tocCHK) == chksz and vrsn == 1 
        curchksz -= 1

    if not validFooter:
        raise Exception("Failed to find valid footer for cdn index file.")

    ents = {}

    dupe=0

    blk_cnt = len(df) // (bs*1024)
    max_el_per_blk = (bs*1024) // 0x18
    for x in range(blk_cnt):
        d.seek(x*bs*1024)
        for _ in range(max_el_per_blk):
            ek=var_int(d,eks,False)
            if ek in ents:
                dupe+=1
                continue

            es=var_int(d,ess,False)
            if ek == 0 or es == 0:
                break
            eo=var_int(d,eos,False)

            e=FileInfo()
            e.offset=eo
            e.compressed_size=es
            e.ekey=ek
            ents[ek] = e

    # print(f"{len(ents)} == {numel} (max is {max_el_per_blk*blk_cnt}, {dupe} dupes)")
    assert len(ents) == numel

    return ents

def save_data( data: bytes, file_name: str ):
    cache_file = os.path.join( CACHE_DIRECTORY, f"{file_name}.cache")

    if not os.path.exists( CACHE_DIRECTORY ):
        os.makedirs( CACHE_DIRECTORY )
    with open( cache_file, "wb", buffering=0 ) as f:
        f.write( data )

class CASCReader:
    ckey_map:Dict[int,int]
    listed_files:Dict[int,bytes]
    file_table:Dict[int,FileInfo]
    file_translate_table:Dict[int,tuple]

    def __init__(self, read_install_file: bool = True):
        if read_install_file:
            ine = parse_install_file(self.get_file_by_ckey(self.install_ckey))
            for x in ine:
                self.file_translate_table.append((NAMED_FILE,x.name,f"{x.md5:x}"))

        for ckey in self.ckey_map:
            first_ekey = self.ckey_map[ckey]
            if first_ekey in self.file_table:
                fi = self.file_table[first_ekey]
                fi.ckey = ckey

        for x in self.file_translate_table:
            if isinstance(x[2],bytes):
                ckey = int.from_bytes(x[2],byteorder='big')
            else:
                ckey = int(x[2],16)
            fi = self.get_file_info_by_ckey(ckey)
            if fi is None:
                continue
            if x[0] is NAMED_FILE:
                fi.name=x[1]
            elif x[0] is WOW_DATAID_FILE:
                fi.extras = {"data_id": x[1]}
                if self.listed_files is not None:
                    if x[1] in self.listed_files:
                        fi.name = self.listed_files[x[1]] 
                    else:
                        fi.name = "FILE_BY_ID/"+str(x[1])

    def get_name(self,ckey):
        fi = self.get_file_info_by_ckey(ckey)
        if fi is not None:
            return fi.name if hasattr(fi,"name") else None
        return None

    def list_files( self ) -> list:
        """Returns a list of tuples, each tuple of format (FileName, CKey)"""
        files = []
        for x in self.ckey_map:
            first_ekey = self.ckey_map[x]
            if first_ekey in self.file_table: # check if the ckey_map entry is inside the file.
                finfo = self.get_file_info_by_ckey(x)
                if finfo is not None and hasattr(finfo,'name'):
                    files.append((finfo.name,x))
        return files
    
    def list_unnamed_files( self ) -> list:
        """Returns a list of tuples, each tuple of format (Ckey,Ckey) (to match with named files list)"""
        files = []
        for ckey in self.ckey_map:
            first_ekey = self.ckey_map[ckey]
            if first_ekey in self.file_table:
                finfo = self.get_file_info_by_ckey(ckey)
                if finfo is not None and not hasattr( finfo, 'name' ):
                    files.append( ( ckey, ckey ) )
        return files

    def get_file_size_by_ckey(self,ckey):
        raise NotImplementedError()

    def get_chunk_count_by_ckey(self,ckey):
        raise NotImplementedError()

    def get_file_by_ckey(self, ckey, max_size = -1):
        raise NotImplementedError()

    def get_file_info_by_ckey(self,ckey: Union[int,str]):
        raise NotImplementedError()

    def is_file_fetchable(self,ckey,include_cdn=True):
        raise NotImplementedError()

    def on_progress(self,step,pct):
        """ Override me! 
        This function receives progress update events for anything that takes time in the program.
        **Not implemented yet** """
        pass


from PyCASC.launcher import getProductCDNFile, getProductVersions, isCDNFileCached
from PyCASC.utils.blizzutils import parse_build_config
from PyCASC.utils.CASCUtils import parse_blte

class CDNCASCReader( CASCReader ):
    """웹에 등록된 파일시스템 정보 로더"""

    def __init__( self, product: str, region: str = "us", read_install_file: bool = False ):
        """원격지 파일시스템 읽기"""

        self.product = product

        vrs = [ x for x in getProductVersions( product ) if x['Region'] == region ]
        if len(vrs)==0:
            raise Exception(f"Product {product} or Region {region} invalid. Cannot load CASC data")

        vr = vrs[0]
        bc = vr['BuildConfig']
        bc_f = getProductCDNFile( product, bc, region, ftype="config", enc="utf-8" )
        self.build_config = parse_build_config(bc_f)

        cdn_f = parse_build_config(getProductCDNFile(product,vr['CDNConfig'],region,ftype="config",enc="utf-8"))
        archives = cdn_f['archives'].split()
        self.file_table={} # ckey -> fileinfo, populated over time instead of all at once, unlike DirCASCReader

        for a in archives:
            i = getProductCDNFile(product,a,region,ftype="data",index=True,cache_dur=-1)
            try:
                ed=r_cidx(i)
                for xi in ed:
                    if xi in self.file_table:
                        continue
                    x=ed[xi]
                    x.data_file=a
                    self.file_table[xi] = x
            except AssertionError as e:
                print("archive index file " + a + " did not match assertions, ignoring this for now since it only causes minor issues.")
                # raise e
                
        print(f"[ETBL] {len(self.file_table)}")

        self.uid = self.build_config['build-uid']
        root_ckey = self.build_config['root']
        enc_hash1,enc_ekey = self.build_config['encoding'].split()
        self.install_ckey,_ = self.build_config['install'].split()
        download_hash1,_ = self.build_config['download'].split()
        size_hash1,_ = self.build_config['size'].split()

        encfile = getProductCDNFile(product,enc_ekey,region,ftype="data",cache_dur=-1) # enc files never change. not that i know of
        encfile = parse_blte(encfile)[1]

        self.ckey_map = parse_encoding_file(encfile,whole_key=True)
        print(f"[CTBL] {len(self.ckey_map)}")

        root_file = self.get_file_by_ckey(root_ckey)
        self.file_translate_table = parse_root_file(self.uid,root_file,self) # maps some ID(can be filedataid, path, whatever) -> ckey
        print(f"[FTTBL] {len(self.file_translate_table)}")

        self.file_translate_table.append((NAMED_FILE,"_ROOT",root_ckey))
        
        self.ckey_map[int(enc_hash1,16)] = int(enc_ekey,16) # map the encoding file's ckey to its own ekey on the ckey-ekey map, since it appears to not be included in the enc-table
        self.file_translate_table.append((NAMED_FILE,"_ENCODING",enc_hash1))
        self.file_translate_table.append((NAMED_FILE,"_INSTALL",self.install_ckey))
        self.file_translate_table.append((NAMED_FILE,"_DOWNLOAD",download_hash1))
        self.file_translate_table.append((NAMED_FILE,"_SIZE",size_hash1))

        if product == "wow":
            if LISTFILE[1] == "82":
                self.listed_files = prep_82_listfile(LISTFILE[0])
            else:
                self.listed_files = prep_6x_listfile(LISTFILE[0])
                
        CASCReader.__init__(self, read_install_file)

        if product == "wow":
            tk_list = []
            tk_lookup = []
            for x in self.listed_files:
                if self.listed_files[x].lower() == "dbfilesclient/tactkey.db2":
                    pass
                elif self.listed_files[x].lower() == "dbfilesclient/tactkeylookup.db2":
                    pass

    def get_file_info_by_ckey(self, ckey):
        if isinstance(ckey,str):
            ckey=int(ckey,16)

        if ckey not in self.ckey_map:
            return None

        if self.ckey_map[ckey] in self.file_table:
            finfo = self.file_table[self.ckey_map[ckey]]
            if not hasattr(finfo,"ckey"):
                finfo.ckey = ckey
            return finfo
        else:
            if ckey not in self.ckey_map:
                return None
            fi = FileInfo()
            fi.ckey = ckey
            fi.ekey = self.ckey_map[ckey]
            self.file_table[fi.ekey]=fi
            return fi

    def _get_file_blte(self,finfo,with_data=True,max_size=-1):
        from requests.exceptions import HTTPError
        if hasattr(finfo,"data_file") and finfo.data_file is not None:
            # archives never expire
            archive_file = getProductCDNFile(self.product,finfo.data_file,cache_dur=-1,offset=finfo.offset,size=finfo.compressed_size)
            return parse_blte(archive_file) #[finfo.offset:finfo.offset+finfo.compressed_size]
        else:
            ekey = f"{finfo.ekey:032x}"
            # print(ekey,f"{finfo.ckey:032x}")
            # These files should also never expire, since if they did their ckey would be different. 
            #  but for sanity i'll keep for 10 days
            return parse_blte(getProductCDNFile(self.product,ekey,max_size=max_size,cache_dur=3600*24*10),read_data=with_data)

    def _populate_file_info_sizes(self,finfo):
        blte_header,_ = self._get_file_blte(finfo,with_data=False)
        finfo.chunk_count=len(blte_header[3])
        finfo.uncompressed_size=0
        for c in blte_header[3]: # for each chunk
            finfo.uncompressed_size+=c[1]

    def get_file_size_by_ckey(self, ckey):
        try:
            finfo = self.get_file_info_by_ckey(ckey)
            if finfo == None:
                return None
            if not hasattr(finfo,"uncompressed_size") or finfo.uncompressed_size is None:
                self._populate_file_info_sizes(finfo)
            return finfo.uncompressed_size
        except:
            return 0
    
    def get_chunk_count_by_ckey(self, ckey):
        try:
            finfo = self.get_file_info_by_ckey(ckey)
            if finfo == None:
                return None
            if not hasattr(finfo,"uncompressed_size") or finfo.uncompressed_size is None:
                self._populate_file_info_sizes(finfo)
            return finfo.chunk_count
        except:
            return 0

    def get_file_by_ckey(self,ckey,max_size=-1):
        finfo = self.get_file_info_by_ckey(ckey)
        if finfo is None:
            return None
        return self._get_file_blte(finfo,max_size=max_size)[1]
    
    def is_file_fetchable(self, ckey, include_cdn=True):
        if include_cdn:
            return self.get_file_info_by_ckey(ckey) is not None
        else:
            finfo = self.get_file_info_by_ckey(ckey)
            if finfo is None:
                return False
            if hasattr(finfo,"data_file") and finfo.data_file is not None:
                return isCDNFileCached(self.product,finfo.data_file,cache_dur=-1)
            else:
                ekey = f"{finfo.ekey:032x}"
                return isCDNFileCached(self.product,ekey,cache_dur=3600*24*10)


class DirCASCReader( CASCReader ):
    """로컬에 설치된 CASC 파일 시스템 로더"""

    def __init__( self, base_path: str, read_install_file: bool=False ):

        if ( not os.path.exists( base_path.rstrip('/') + "/.build.info" ) or
             not os.path.exists( base_path.rstrip('/') + "/Data/data" ) ):
            raise Exception("Not a valid CASC datapath")

        self.base_path      = base_path.rstrip('/')
        """설치 폴더경로"""
        self.build_path     = self.base_path + "/.build.info"
        """.build.info 파일경로"""
        self.data_path      = self.base_path + "/Data/data/"
        """데이터 폴더경로"""

        build_file          = None
        self.build_config   = None

        # .build.info 파일 읽기
        with open( self.build_path, "r" ) as f:
            build_file_bin  = f.read()
            build_file_dat  = parse_config( build_file_bin )
            build_file      = build_file_dat[0]
            print( "[.build.info] : ", build_file )
            #build_file = parse_config(b.read())[0]

        # /Data/config/[Build Key] 파일 읽기
        self.build_config_path = self.base_path + "/Data/config/" + prefix_hash( build_file['Build Key'] )
        with open( self.build_config_path, "r" ) as f:
            build_config_bin    = f.read()
            build_config_dat    = parse_build_config( build_config_bin )
            self.build_config   = build_config_dat
            print( "[.build.config]", self.build_config )
            #self.build_config = parse_build_config(b.read())

        print("[BF]")

        assert build_file is not None and self.build_config is not None

        self.uid            = self.build_config['build-uid']
        root_ckey           = self.build_config['root']
        enc_hash1,enc_hash2 = self.build_config['encoding'].split()
        self.install_ckey,_ = self.build_config['install'].split()
        download_hash1,_    = self.build_config['download'].split()
        size_hash1,_        = self.build_config['size'].split()

        self.file_table     = {} # maps ekey -> fileinfo (size, datafile, offset)

        self.load_idx_files( self.file_table )

        self.load_encoding_file( self.file_table, enc_hash2 )

        self.load_root_file( root_ckey )

        self.file_translate_table.append((NAMED_FILE,"_ROOT",root_ckey))
        
        self.ckey_map[int(enc_hash1,16)] = int(enc_hash2[:18],16) # map the encoding file's ckey to its own ekey on the ckey-ekey map, since it appears to not be included in the enc-table
        self.file_translate_table.append((NAMED_FILE,"_ENCODING",enc_hash1))
        self.file_translate_table.append((NAMED_FILE,"_INSTALL",self.install_ckey))
        self.file_translate_table.append((NAMED_FILE,"_DOWNLOAD",download_hash1))
        self.file_translate_table.append((NAMED_FILE,"_SIZE",size_hash1))

        self.load_listfile()

        CASCReader.__init__(self, read_install_file)

    def load_listfile( self ):
        """리스트파일 로딩, ( FileID, FilePath ) 목록"""
        if self.uid == "wow":
            if LISTFILE[1] == "82":
                self.listed_files = prep_82_listfile(LISTFILE[0])
            else:
                self.listed_files = prep_6x_listfile(LISTFILE[0])

    def load_root_file( self, root_ckey ):
        """루트 파일 로딩, ( FileID, CKey, NameHash ) 목록"""
        print( root_ckey, self.ckey_map[int(root_ckey,16)], self.file_table[self.ckey_map[int(root_ckey,16)]] )
        root_file = self.get_file_by_ckey( root_ckey )

        save_data( root_file, root_ckey )
        
        self.file_translate_table = parse_root_file( self.uid, root_file, self ) # maps some ID(can be filedataid, path, whatever) -> ckey
        print(f"[FTTBL] {len(self.file_translate_table)}")

    def load_encoding_file( self, file_table, enc_hash2 ):
        """인코딩 파일 로딩, ( CKey, EKey ) 목록"""
        enc_info = file_table[ int( enc_hash2[:18], 16 ) ]
        enc_file = r_cascfile( self.data_path, enc_info.data_file, enc_info.offset )

        save_data( enc_file, enc_hash2 )

        # Load the CKEY MAP from the encoding file.
        self.ckey_map = parse_encoding_file( enc_file ) # maps ckey(hexstr) -> ekey(int of first 9 bytes)
        print( f"[CTBL] { len( self.ckey_map ) }" )

    def load_idx_files( self, file_table ):
        """idx 파일 로딩, ( EKey, DataIndex, Offset, Size ) 목록"""
        idx_files_recent    = []
        idx_files           = fnmatch.filter( os.listdir( self.data_path ), "*.idx" )
        for i in range( 0x10 ):
            idxs            = fnmatch.filter( idx_files, f"{i:02x}*.idx" )
            idx_files_recent.append( idxs[-1] ) # last one

        print( idx_files_recent )

        for x in idx_files_recent:
            ents            = r_idx( self.data_path + x )
            for e in ents:
                if e.ekey not in file_table:
                    file_table[ e.ekey ] = e

        print( f"[전체에셋] : { len( file_table ) }" )

    
        
    def get_file_size_by_ckey(self,ckey):
        finfo = self.get_file_info_by_ckey(ckey)
        if finfo == None:
            return None
        if not hasattr(finfo,"uncompressed_size") or finfo.uncompressed_size is None:
            finfo.uncompressed_size, finfo.chunk_count = cascfile_size(self.data_path,finfo.data_file,finfo.offset)
        return finfo.uncompressed_size

    def get_chunk_count_by_ckey(self,ckey):
        finfo = self.get_file_info_by_ckey(ckey)
        if finfo == None:
            return None
        if not hasattr(finfo,"chunk_count") or finfo.chunk_count is None:
            finfo.uncompressed_size, finfo.chunk_count = cascfile_size(self.data_path,finfo.data_file,finfo.offset)
        return finfo.chunk_count

    def get_file_by_ckey(self,ckey,max_size=-1):
        finfo = self.get_file_info_by_ckey(ckey)
        if finfo is None:
            return None
        return r_cascfile(self.data_path,finfo.data_file,finfo.offset,max_size)
    
    def get_file_info_by_ckey(self, ckey):
        """Takes ckey in either int form or hex form"""
        if isinstance(ckey,str):
            ckey=int(ckey,16)

        try:
            return self.file_table[self.ckey_map[ckey]]
        except:
            return None

    def is_file_fetchable(self, ckey, include_cdn=True):
        finfo = self.get_file_info_by_ckey(ckey)
        return finfo is not None

if __name__ == '__main__':
    import cProfile, io
    from pstats import SortKey,Stats
    pr = cProfile.Profile()
    pr.enable()

    # On my pc, these are some paths:
    # cr = DirCASCReader("G:/Misc Games/Warcraft III") # War 3
    # cr = DirCASCReader("G:/Misc Games/Diablo III") # Diablo 3
    # On my mac, these are the paths:
    # cr = DirCASCReader("/Users/raid/Diablo III") #Diablo 3
    # cr = DirCASCReader("/Applications/Warcraft III") # War 3

    # war3, diablo3, hots, sc2,  ow, hearthstone, wow,  wowclassic,
    #   w3,      d3, hero,  s2, pro,         hsb, wow, wow_classic,

    # supposedly supported: w3, d3

    # hero is mndx, same with s2
    cr = CDNCASCReader("s2") # Read s2 CASC dir from CDN.
    print(f"{len(cr.list_files())} named files loaded in list")

    pr.disable()
    s = io.StringIO()
    sortby = SortKey.CUMULATIVE
    ps = Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    print(s.getvalue())
    import time
    time.sleep(15)