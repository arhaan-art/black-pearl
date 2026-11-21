#!/usr/bin/env python3
# -*-coding:UTF-8 -*
"""
The Exif Module
======================

"""

##################################
# Import External packages
##################################
import os
import sys

from PIL import Image, ExifTags, UnidentifiedImageError

sys.path.append(os.environ['AIL_BIN'])
##################################
# Import Project packages
##################################
from modules.abstract_module import AbstractModule


class Exif(AbstractModule):
    """
    CveModule for AIL framework
    """

    def __init__(self):
        super(Exif, self).__init__()

        # Waiting time in seconds between to message processed
        self.pending_seconds = 1

        # Send module state to logs
        self.logger.info(f'Module {self.module_name} initialized')

    def compute(self, message):
        image = self.get_obj()
        try:
            img = Image.open(image.get_filepath())
            img_exif = img.getexif()
            if img_exif:
                self.logger.info(f'Exif: {self.get_obj().id}')
                # Persist canonical EXIF via set_file_meta (was: print + sys.exit(0))
                try:
                    sys.path.insert(0, os.path.join(os.environ.get('AIL_BIN', ''), 'lib', 'ingest', 'processors'))
                    from metadata import extract_exif  # type: ignore
                    with open(image.get_filepath(), 'rb') as fh:
                        meta, _w = extract_exif(fh.read())
                except Exception:
                    meta = {}
                    for key, val in img_exif.items():
                        name = ExifTags.TAGS.get(key, str(key))
                        meta[f'exif.{name}'] = str(val)
                if meta:
                    try:
                        image.set_file_meta(meta)
                    except Exception as exc:
                        self.logger.warning(f'set_file_meta failed: {exc}')
        except UnidentifiedImageError:
            self.logger.info(f'Invalid image: {image.get_filepath()}')
        except Exception as exc:
            self.logger.warning(f'Exif compute failed: {exc}')

        # tag = 'infoleak:automatic-detection="cve"'
        # Send to Tags Queue
        # self.add_message_to_queue(message=tag, queue='Tags')


if __name__ == '__main__':

    module = Exif()
    module.run()
