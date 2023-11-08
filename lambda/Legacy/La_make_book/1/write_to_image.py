import textwrap
from PIL import Image, ImageDraw, ImageFont

def write_to_image(text, width, font_path, font_color, result_path):

    para = textwrap.wrap(text, width=width) #텍스트 한 줄에 최대 width자

    MAX_W, MAX_H = 1024, 1024 #이미지 사이즈
    font_size = 10


    im=Image.new("RGB",(1024,1024),(255,255,255))
    draw = ImageDraw.Draw(im)
    font = ImageFont.truetype(font_path, size=font_size) #글씨체 및 글씨 크기 설정
    font_color = 'rgb(0, 0, 0)' #글씨 색 설정 -> 검정색


    breakpoint = 0.7 * im.size[0] #get width
    max_leng = -1
    it = 0
    i = 0
    for line in para:
        if max_leng < len(line):
            max_leng = len(line)
            it = i
        i += 1

    """
    Binary search-ish for finding optimal font size that fit to the breakpoint
    """

    jumpsize = 75
    while True:
        font = ImageFont.truetype(font_path, size=font_size)
        _, _, w, h = draw.textbbox((0, 0), para[it], font=font)
        if (w < breakpoint) :
            font_size += jumpsize
        else:
            jumpsize = jumpsize // 2
            font_size -= jumpsize
        font = ImageFont.truetype(font_path, size=font_size)
        if (jumpsize <= 1) :
            break

    pad = 10

    font = ImageFont.truetype(font_path, size=font_size)

    _, _, _, h = draw.textbbox((0, 0), para[0], font=font)
    leng = len(para) * h + pad * (len(para) - 1)
    current_h = (MAX_H - leng)/2

    print(leng,current_h)
    for line in para:
        _, _, w, h = draw.textbbox((0, 0), line, font=font)
        draw.text(((MAX_W - w) / 2, current_h), line, font=font, fill=font_color) # 가운데 정렬
        current_h += h + pad #다음 줄이 작성될 위치

    im.save(result_path) #test.png 로 저장