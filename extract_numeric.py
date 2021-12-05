import re
a='Posted by Editor - Technology News | Dec 5, 2020 | Technology | 0 | Tesla’s top leadership positions skew white and male with just 4% of those roles going to Black employees, according to the company’s first diversity and inclusion report released Friday.'
ans=" ".join(re.split("[^0-9]*",a))
print(ans)