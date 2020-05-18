#!/usr/bin/env python
#+
# Name:
#   era_interim_download_main
# Purpose:
#   A python procedure to download a ton of data

import subprocess as sp;

cmd = ['screen', '-ls'];

process = sp.Popen( cmd, stdout = sp.PIPE);
code    = process.wait();
info    = process.stdout.read();

screens = ['era_an_pl', 'era_an_sfc', 'era_fc_sfc'];

cmd = ['screen', '-dmS', '', '']

dir = './python/ERA_Scripts/downloading/';
if '.era_an_pl' not in info:
	print 'starting era_an_pl screen';
	cmd[2] = 'era_an_pl';
	cmd[3] = dir + 'era_interim_download_an_pl.py';
	x = sp.call( cmd );
if '.era_an_sfc' not in info:
	print 'starting era_an_sfc screen';
	cmd[2] = 'era_an_sfc';
	cmd[3] = dir + 'era_interim_download_an_sfc.py';
	x = sp.call( cmd );
if '.era_fc_sfc' not in info:
	print 'starting era_fc_sfc screen';
	cmd[2] = 'era_fc_sfc';
	cmd[3] = dir + 'era_interim_download_fc_sfc.py';
	x = sp.call( cmd );

exit(0);