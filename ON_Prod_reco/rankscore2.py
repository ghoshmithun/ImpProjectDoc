import sys

for line in sys.stdin:
	masterkey,rankscore_p,no_hits = line.strip().split('\t')
	ans=0.0
	for i in range(int(no_hits)+1):
		if i==0:
			ans=0.0
		else:
			ans=ans + (1.0 /(2**(i-1)))	
    	print '\t'.join([masterkey,rankscore_p,no_hits,str(ans)])					 

