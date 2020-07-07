import logging
import os

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler( logging.StreamHandler() )
log.handlers[0].setFormatter(
  logging.Formatter('%(asctime)s [%(levelname)-.4s] %(message)s')
)

HOME   = os.path.expanduser( '~' )
LOGDIR = os.path.join( HOME, 'Library', 'Application Support', __name__ )

ERAI_LOGDIR = os.path.join( LOGDIR, 'ERAI' )
