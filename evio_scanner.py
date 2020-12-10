#!/usr/bin/env python
#
# evio_scanner.py - script to scan an evio raw data file and record
#                   the offset into the file of the beginning of data
#                   blocks at regular intervals.
#
# author: richard.t.jones at uconn.edu
# version: november 12, 2020

import sys
import struct
import time

pitch = 10
print_level = 0

contentTypes = ["unknown", "unsigned int", "float", "char*",
                "short", "unsigned short", "char", "unsigned char",
                "double", "long int", "long unsigned int", "int",
                "TAGSEGMENT", "SEGMENT", "BANK", "COMPOSITE",
                "BANK", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
                "SEGMENT", "Hollerit*", "N-value*"]

def scan_segment(fin, level):
   try:
      buf = fin.read(4)
      head = struct.unpack(">BBH", buf)
   except:
      print "broken segment at level", level
      return 1e99
   length = head[2]
   btag = head[0]
   btype = head[1] & 0x3f
   if level <= print_level:
      print "".join([" " for i in range(0, level)]),
      print "bank segment tag", btag, "of", length, "words",
      print "of", contentTypes[btype]
   fin.read(4 * length)
   return length + 1

def scan_tagsegment(fin, level):
   try:
      buf = fin.read(4)
      head = struct.unpack(">BBH", buf)
   except:
      print "broken tagsegment at level", level
      return 1e99
   length = head[2]
   btag = head[0]
   btype = head[1] & 0xf
   if level <= print_level:
      print "".join([" " for i in range(0, level)]),
      print "bank tagsegment tag", btag, "of", length, "words",
      print "of", contentTypes[btype]
   fin.read(4 * length)
   return length + 1

def scan_bank(fin, level):
   try:
      buf = fin.read(8)
      head = struct.unpack(">IHBB", buf)
   except:
      print "broken bank at level", level
      return 1e99
   length = head[0] - 1
   btag = head[1]
   btype = head[2] & 0x3f
   if level <= print_level:
      print "".join([" " for i in range(0, level)]),
      print "bank tag", btag, "of", length, "words",
      print "of", contentTypes[btype]
   words = 0
   while words < length:
      if btype == 0xe or btype == 0x10: # BANK
         words += scan_bank(fin, level + 1)
      elif btype == 0xd or btype == 0x20: # SEGMENT
         words += scan_segment(fin, level + 1)
      elif btype == 0xc: # TAGSEGMENT
         words += scan_tagsegment(fin, level + 1)
      elif btype == 0xf: # COMPOSITE
         words += scan_composite(fin, level + 1)
      else:
         fin.read(length * 4)
         words += length
   if words != length:
      print "bank overflow, giving up"
      return 1e99
   return length + 2

for evio in sys.argv[1:]:
   print "scanning", evio
   fin = open(evio, "rb")
   offset = 0
   count = 0
   while True:
      try:
         buf = fin.read(32)
         head = struct.unpack(">IIIIIIII", buf)
      except:
         break
      if head[7] != 0xc0da0100:
         print "corrupted block at count", count
         for c in buf:
            print hex(ord(c)),
         print
      length = head[0] - 8
      print "new chunk of", length, "words"
      print "    * block words", head[0]
      print "    * block number", head[1]
      print "    * header length", head[2]
      print "    * event count", head[3]
      print "    * Reserved 1", head[4]
      print "    * version", head[5]
      print "    * Reserved 2", head[6]
      print "    * magic", hex(head[7])
      words = 0
      while words < length:
         words += scan_bank(fin, 0)
      offset += head[0] * 4
      #fin.seek(offset)
      count += 1
   try:
      fin.close()
   except:
      continue
