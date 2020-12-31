# mvjep_001

## Título

Duração incorreta de video afetando o sumário

## Descrição
Alguns vídeos possuem um metadado de duração maior do que sua duração realmente executável, por serem arquivos de vídeos corrompidos parcialmente.
Este erro confunde o processo de formação do sumário, gerando playlist incorretamente.

## Testes
Usando o app ffmpeg:
Para checkar se um video está corrompido, o comando abaixo resolve:\
`ffmpeg -v error -i file.avi -f null - 2>error.log`

Para 'descorromper' o arquivo, gerando uma cópia de mesmo tamanho, mas com metadado de duração correto, o comando abaixo resolve:\
`ffmpeg -i "A Consciência de Imortalidade-2010-001.mp4" -c:v copy -c:a copy output.mp4`

## Proposta de correcao
No script video_tools, adaptar função 'join_mp4()' , para:
- [x] Retornar lista de dict, com chaves 'caminho original do video', e 'duração do vídeo após convertido para extensão .ts', sendo esta, sua duração real.

No script mass_videojoin, adaptar função join_videos(), para:
- [x] carregar em variável 'list_dict_real_duration', retorno da função join_mp4()
- [x] registrar a duração real desses vídeos na coluna 'video_duration_real' na planilha de controle. Formatação: hhh:mm:ss.ms
- [x] após o processamento de todos os vídeos,
	- substituir o rótulo da coluna 'duration' para 'video_origin_duration_pre_join'.
	- Substituir o rotulo da coluna 'video_duration_real' para 'duration'.


## Referências de pesquisa
- https://www.xspdf.com/resolution/50747510.html
- https://superuser.com/questions/100288/how-can-i-check-the-integrity-of-a-video-file-avi-mpeg-mp4
