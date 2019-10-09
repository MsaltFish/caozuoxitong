#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<math.h>
#include<time.h>
#include<pthread.h> 
#include <semaphore.h>
#include <unistd.h> 
#include   <sys/time.h>
#define   THREAD_MAX_NUM   3
int   send_num;   
int   send_count;   
pthread_mutex_t   mut   =   PTHREAD_MUTEX_INITIALIZER;   
pthread_t   thread[THREAD_MAX_NUM];   
 
int   lineNum;   
FILE*   file_in   ;   
char   oneline[100];

void*   sub_thread(void   *arg)   
{   
    printf("现在在线程   %d   中,send_num   =   %d\n",pthread_self(),send_num);   
    while(lineNum<10000)   
    {   
        pthread_mutex_lock(&mut);   
        fgets(   oneline,   40,   file_in   );   
        lineNum++;   
        pthread_mutex_unlock(&mut);   
 
        //这里进行计数操作   
        printf("the   %d   th   line   :   %s\n",lineNum-1,oneline);   
    }// for(int   i=0;i<100;i++)   
    // printf("%d",i);   
    return(0);   
}


int   create_thread()   
{   
    int   i=0,temp;   
    // pthread_t   threadid[THREAD_MAX_NUM];   
    for(i=0;i<THREAD_MAX_NUM;i++)   
    {   
    //    printf("pthread_create(%d)\n",i);   
        pthread_create(&thread[i],NULL,sub_thread,NULL);   
        pthread_mutex_lock(&mut);   
        //send_num++;   
        pthread_mutex_unlock(&mut);   
    }   
    temp   =   i;   
    for(i=0;i<temp;i++)   
    {   
        pthread_join(thread[i],NULL);   
    //    printf("Thread   %d   down\n",i);   
    }   
    return   i;   
}  


typedef struct
{
	int c_custkey;    	   //顾客编号
	char c_mkgsegment[20]; //对应的某个市场部门
}customer;				   //顾客结构体 

typedef struct
{
	int o_orderkey;    	 //订单号 
	int o_custkey;    	 //顾客编号
	char o_orderdate[10];//订货日期 
}orders;				 //订单

typedef struct
{
	int l_orderkey;//订单号
	double l_extendedprice;//额外价格
	char l_shipdate[10];//发货日期 
}lineitem; //商品信息 

typedef struct
{
	int l_orderkey;//订单号
	char o_orderdate[10];//订货日期 
	double l_extendedprice;//额外价格
}select_result;
typedef struct
{
	customer * cus;
	orders * ord;
	lineitem * item;
	char * order_date;
	char * ship_date;
	char * mktsegment;
	int range[2];	
}argg;

select_result *re2;
static sem_t sem_one;
//int count=0;


void * read_customer_txt() //读取customer。txt内容 
{
	FILE * fp;
	int   thnum;  
	customer *a=NULL;
	lineNum=0; 
	a = (customer *)malloc(105*sizeof(customer));
	int i=0;
	char b;
	fp = fopen("C:\\Users\\78332\\Desktop\\马志立4042017021\\Sourcecode\\customer.txt","r");
//	printf("Create   %d   thread   \n",THREAD_MAX_NUM);   
    pthread_mutex_init(&mut,NULL);   
    thnum   =   create_thread(); 
//    printf("down\n");   
 //   printf("thread   main   :   %d",pthread_self());   
 //   printf("thread   main   :   %d",getpid());
	if(NULL==fp)
	{
		printf("cannot open customer.txt!");
		return NULL;
	}
	while(!feof(fp))
	{	
		fscanf(fp,"%d%c%s",&a[i].c_custkey,&b,&a[i].c_mkgsegment);
		i++;
	}
	fclose(fp);
	return a;
}
void * read_orders_txt()//读取orders.txt内容 
{
	int i =0; 
	int   thnum;   
    lineNum=0;   
    send_num   =   0; 
	orders * a=NULL;
	a = (orders * )malloc(4005*sizeof(orders));
	char b,c;
	long d;
	FILE *fp;
	fp = fopen("C:\\Users\\78332\\Desktop\\马志立4042017021\\Sourcecode\\orders.txt","r");
	printf("Create   %d   thread   \n",THREAD_MAX_NUM);   
    pthread_mutex_init(&mut,NULL);   
    thnum   =   create_thread();   
//    printf("down\n");   
//    printf("thread   main   :   %d",pthread_self());   
//    printf("thread   main   :   %d",getpid());
	if(fp == NULL)
	{
		printf("cannot open orders.txt!");
		return NULL;
	}
	while(!feof(fp))
	{	
		fscanf(fp,"%d%c%lld%c%s",&a[i].o_orderkey,&b,&d,&c,&a[i].o_orderdate);
		a[i].o_custkey=d%100;
		i++;
	}
	fclose(fp);
	return a;
}

void * read_lineitem_txt()//读取lineitem.txt内容
{
	FILE * fp;
	int   thnum;   
    lineNum=0;   
    send_num   =   0; 
	lineitem * l=NULL;
	l = (lineitem *)malloc(1005*sizeof(lineitem));
	int i=0;
	char b,c;
	fp = fopen("C:\\Users\\78332\\Desktop\\马志立4042017021\\Sourcecode\\lineitem.txt","r");
	printf("Create   %d   thread   \n",THREAD_MAX_NUM);   
    pthread_mutex_init(&mut,NULL);   
    thnum   =   create_thread();   
//    printf("down\n");   
//    printf("thread   main   :   %d",pthread_self());   
//    printf("thread   main   :   %d",getpid());
	if(fp==NULL)
	{
		printf("cannot open lineitem.txt!");
		return NULL;
	}
	while(!feof(fp))
	{
		fscanf(fp,"%d%c%lf%c%s",&l[i].l_orderkey,&c,&l[i].l_extendedprice,&b,&l[i].l_shipdate);
		i++;
	}
	fclose(fp);
	return l; 
}

void sum(select_result * re1,int num)
{
	
	int i=0,j=0;

		for(i=0;i<num;i++)
		{

			if(re2[0].l_orderkey==0)
			{
			re2[j].l_orderkey = re1[i].l_orderkey;
			strcpy(re2[j].o_orderdate,re1[i].o_orderdate);
			re2[j].l_extendedprice = re1[i].l_extendedprice;
			
			continue;
			}
			if(re1[i].l_orderkey==re2[j].l_orderkey)
			{
				re2[j].l_extendedprice = re2[j].l_extendedprice + re1[i].l_extendedprice;

			}
			else
			{
			
				j++;
				re2[j].l_orderkey = re1[i].l_orderkey;
				strcpy(re2[j].o_orderdate,re1[i].o_orderdate);
				re2[j].l_extendedprice = re1[i].l_extendedprice;
			}
	
		}
}
void * Select(void *arg)//进行选择 
{
	
	argg *arg1;
	arg1=(argg *)arg;
	customer *cus=(customer *)arg1->cus;
	orders *ord=(orders *)arg1->ord;
	lineitem *item=(lineitem *)arg1->item;
	char *order_date=(char *)arg1->order_date;
	char *ship_date=(char *)arg1->ship_date;
	char *mktsegment=(char *)arg1->mktsegment;
	int begin=((int *)arg1->range)[0];
	int end=((int *)arg1->range)[1];
	int i,j,k,l=0;
	select_result * result1=NULL;

	result1 = (select_result *)malloc(340*sizeof(select_result));

	for(j=begin;j<end;j++)
	{
		for(i=0;i<100;i++)
		{
			for(k=0;k<1000;k++)
			if(cus[i].c_custkey==ord[j].o_custkey&&ord[j].o_orderkey==item[k].l_orderkey&&(strcmp(ord[j].o_orderdate,order_date)<0)&&(strcmp(item[k].l_shipdate,ship_date)>0)&&(strcmp(cus[i].c_mkgsegment,mktsegment)==0))
			{

				result1[l].l_orderkey=item[k].l_orderkey;
				strcpy(result1[l].o_orderdate,ord[j].o_orderdate);
				result1[l].l_extendedprice=item[k].l_extendedprice;
				l++;
				
			}
		}
	}

	sum(result1,l);

	free(result1);

	return NULL;//返回选择结果的指针 
}

void * sort()//进行选择 
{
	int i,j,m=0;
	select_result  temp;
	for(i=0;i<m-1;i++)//冒泡排序 ，将选择结果排为降序 
	{
		for(j=0;j<m-1-i;j++)
		{
			//printf("%lf->%lf\n",result2[j].l_extendedprice,result2[j+1].l_extendedprice);
			if(re2[j].l_extendedprice<re2[j+1].l_extendedprice)
			{
				printf("123");
				temp.l_extendedprice=re2[j].l_extendedprice;
				temp.l_orderkey=re2[j].l_orderkey;
				strcpy(temp.o_orderdate,re2[j].o_orderdate);
				re2[j].l_extendedprice=re2[j+1].l_extendedprice;
				re2[j].l_orderkey=re2[j+1].l_orderkey;
				strcpy(re2[j].o_orderdate,re2[j+1].o_orderdate);
				re2[j+1].l_extendedprice=temp.l_extendedprice;
				re2[j+1].l_orderkey=temp.l_orderkey;
				strcpy(re2[j+1].o_orderdate,temp.o_orderdate);
			}
		}
	}
}

int change_argv_to_number(char s[])//将命令行里读入的数字字符串转化为整形数字 
{
	int i=0;
	int number=0;
	while(s[i]!='\0')
	{
		if(i==0)
			number = (s[i]-48);
		else
			number = number*10 + (s[i]-48);
		//printf("%d,%d\n",i,number);
		i++;
	}
	return number;
}

int main(int argc,char * argv[])//argc表示输入内容的总个数，argv[]内保存着输入的内容 
{
	int begintime,endtime;
	int i,j,c,thread,d,p;
	int num;
	pthread_t id[20];

	int limit=3;
	char order_date[15];
	char ship_date[10];
	char mktsegment[20];
	select_result *result=NULL;
	void * cus1 = NULL;//指向客户表的指针 
	void * ord1 = NULL;//指向订单表的指针 
	void * item1 = NULL;//指向 产品表的指针
	customer * cus = NULL;//指向客户表的指针 
	orders * ord = NULL;//指向订单表的指针 
	lineitem * item = NULL;//指向 产品表的指针 
	sem_init(&sem_one, 0, 1);
	argg *arg1;
	arg1=(argg *)malloc(20*sizeof(argg));
	
	
	pthread_create(&id[0], NULL, read_customer_txt, NULL);
    pthread_create(&id[1], NULL, read_orders_txt, NULL);
 	pthread_create(&id[2], NULL, read_lineitem_txt, NULL);//sleep(2);
	

 	
    pthread_join(id[0], &cus1);
    pthread_join(id[1], &ord1);
    pthread_join(id[2], &item1);
    cus=(customer *)cus1;
    ord=(orders *)ord1;
    item=(lineitem *)item1;	

	num = change_argv_to_number(argv[5]);//总共计算的次数
	thread=change_argv_to_number(argv[1]);
	begintime = clock();
	for(i=0;num>0;num--,i=i+4)
	{
		re2=(select_result *)malloc(1000*sizeof(select_result));
		re2[0].l_orderkey=0;
		strcpy(mktsegment,argv[6+i]);
		strcpy(ship_date,argv[8+i]);
		limit = change_argv_to_number(argv[9+i]);

		strcpy(order_date,argv[7+i]);
		d=4000/thread;

		for(c=0;c<thread;c++)
		{
			arg1[c].cus=cus;
			arg1[c].ord=ord;
			arg1[c].item=item;
			arg1[c].order_date=order_date;
			arg1[c].ship_date=ship_date;
			arg1[c].mktsegment=mktsegment;
			if(c==0)
			{
				arg1[c].range[0]=d*c;
				arg1[c].range[1]=d*(c+1);continue;
			} 
			arg1[c].range[0]=d*c+1;
			arg1[c].range[1]=d*(c+1);
			if(c==thread-1)
			{
				arg1[c].range[0]=d*c+1;
				arg1[c].range[1]=d*(c+1)-d*thread+4000;continue;
			}
		}
		for(j=0;j<thread;j++)
		{
			p=pthread_create(&id[j+3], NULL, Select, (void *)&arg1[j]);

		}
		for(j=0;j<thread;j++)
		{
			pthread_join(id[j+3], NULL);
		}
		sort();
		printf("l_orderkey|o_orderdate|revenue\n");
		for(j=0;j<limit;j++)
		{
			if(re2[j].l_extendedprice==0)
				printf("null      |null       |null   \n");
			else
				printf("%-10d|%-11s|%-20.2lf\n",re2[j].l_orderkey,re2[j].o_orderdate,re2[j].l_extendedprice);
		}
		free(re2);
		//result = NULL;
	} endtime = clock();printf("\n\nRunning Time：%dms\n", endtime-begintime);

	
	free(cus1);
	free(ord1);
	free(item1);
	free(arg1);

	sem_destroy(&sem_one);
	return 0;
}

