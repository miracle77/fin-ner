import  re
ss = '<span id="comp">abc交行</span>wordlibabc交行<span id="comp">abc交行</span>abc交行<span id="comp">abc交行</span>'
p1=re.compile(r'(?<=<span id="comp">).*?(?=</span>)')
res = p1.findall(ss)
for word in res:
    print(word)
