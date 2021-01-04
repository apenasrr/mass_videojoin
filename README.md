# Mass_videojoin
## A smart tool to optimize and make turbo join in a massive video collection

## Feature:
- Homogenize resolutions and codecs from filling in spreadsheet
- Make turbo join, 400 times faster than common join
- Join can contain transition effect self-adapted for any video resolution
- Join process could respect:
	- Defined maximum sized per file, generating ordered video blocks, automatically split videos larger than the defined maximum size
	- Personal grouping criteria, like modules for video course
	- Transition effect auto adapted to the videos resolutions

## Notes:
- Works with video .mp4, .webm, .avi, .ts, .vob, .mov, .mkv, .wmv
- Require ffmpeg enabled on path system variables

## How to use
If it's your first time using the tool
1. Execute update_libs.bat

For the next times\
2. Execute mass_videojoin.bat\
3. Navigate through the iterative menu, applying the ordered steps of the process

## News:
- 2021/01/03: Feature join transition effect available. Need to put 2 transition videos in 'transition' folder: One with 16x9 aspect ratio and other with 4x3 aspect ratio. Also need to set the config/config.txt file as activate_transition='true'.
- 2020/12/31: Video duration capturing starts using a method that validates corrupted video and captures only the executable video duration. More details: [mvjep_001](mvjep/mbjep_001.md)
- 2020/11/11: Implemented function of resuming the process 'videos reencode' after previous interruption. Need to re-encode 50 hours of video? Easy! Process a little each day until all the work is done. :)

## Future features:
- Resuming the process 'join videos' after previous interruption.

---
Do you wish to buy a coffee to say thanks?\
LBC (from LBRY) digital Wallet

> bFmGgebff4kRfo5pXUTZrhAL3zW2GXwJSX

### We recommend:
[mises.org](https://mises.org/) - Educate yourself about economic and political freedom\
[lbry.tv](http://lbry.tv/) - Store files and videos on blockchain ensuring free speech\
[A Cypherpunk's Manifesto](https://www.activism.net/cypherpunk/manifesto.html) - How encryption is essential to Free Speech and Privacy