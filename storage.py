# -*- coding: utf-8 -*-

import sys, os, time

from link import Link

def get_hash_path(hash, root):
    path = os.path.join(root, hash[:2], hash[2:4], hash[4:])
    path = os.path.expandvars(path)
    path = os.path.expanduser(path)
    return path

def save_page(page, root):
    path = get_hash_path(page.link.hash(), root)
    path_dir = os.path.dirname(path)
    if not os.path.isdir(path_dir):
        os.makedirs(path_dir)
    f = open(path, 'wb')
    f.write(page.html_content)
    f.close()

