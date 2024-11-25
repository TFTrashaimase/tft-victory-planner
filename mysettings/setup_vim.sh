# #!/bin/bash

# # 비밀번호 설정 없애기
# # echo "airflow ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# # 필요한 디렉터리 권한 설정
# # sudo mkdir -p /var/lib/apt/lists/partial
# # sudo chmod 755 /var/lib/apt/lists
# # sudo chmod 755 /var/lib/apt/lists/partial

# # Vim 설치 (Ubuntu/Debian 기준)
# echo "airflow" | sudo -S apt update && echo "airflow" | sudo -S apt install -y vim

# # ~/.vimrc 파일 작성
# cat <<EOF > ~/.vimrc
# set number   
# set ai    
# set si
# set cindent    
# set shiftwidth=4   
# set tabstop=4    
# set ignorecase   
# set hlsearch   
# set nocompatible    
# set fileencodings=utf-8,euc-kr    
# set fencs=ucs-bom,utf-8,euc-kr   
# set bs=indent,eol,start   
# set ruler  
# set title    
# set showmatch   
# set wmnu    
# syntax on  
# filetype indent on  
# set mouse=a   
# EOF

# # 설치 완료 메시지
# echo "Vim completed"

# # Ariflow 유저로 전환
# # echo "Switching to airflow user..."
# # chown -R airflow:airflow /opt/airflow
# # su - airflow -c "echo 'airflow user switched!'"