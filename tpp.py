divs={1:1}

def fct(n):
    if(n in divs):
        return divs[n]
    else:
        c=0
        i=2
        while(i*i<=n):
            if(n%i==0):
                c+=1
                if(i*i!=n):
                    c+= 1
            i+=1
        divs[n]=c+2
        return divs[n]

n=int(input())
mts=list(map(int,input().split()))
m=int(input())
mts=sorted(list(map(fct,mts)))
i=0
sums=0
while(i<len(mts) and sums<=m):
    sums+=mts[i]
    i+=1
print(i)




if(len(a)>len(b)):
        val=a[len(b)-1]
        for i in range(len(b),len(a)):
            if(a[i]==val):
                a[i]="DEL"
    a=list(filter(lambda x: x!="DEL",a))