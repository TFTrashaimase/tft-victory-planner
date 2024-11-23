#!/bin/bash

# Vim 설치 (Ubuntu/Debian 기준)
sudo apt update && sudo apt install -y vim

# ~/.vimrc 파일 작성
cat <<EOF > ~/.vimrc
set number   
set ai    
set si
set cindent    
set shiftwidth=4   
set tabstop=4    
set ignorecase   
set hlsearch   
set nocompatible    
set fileencodings=utf-8,euc-kr    
set fencs=ucs-bom,utf-8,euc-kr   
set bs=indent,eol,start   
set ruler  
set title    
set showmatch   
set wmnu    
syntax on  
filetype indent on  
set mouse=a   
EOF

# 설치 완료 메시지
echo "Vim completed"
