#include <stdio.h>
#include <default.c>

//#define PRINT_HEAD_TAIL
//#define RELOAD

/* T6 Target format:
	DATE	time;	
	float fHigh, fLow;	// f1,f2
	float fOpen, fClose;	// f3,f4	
	float fVal, fVol;		// f5,f6
*/

// Format for QuantDataManager
// Zorro is expecting the volume field f6 in the input, 
string Format = "+%Y%m%d,%H:%M:%S,f3,f1,f2,f4,f6";

string cutoff_after6(string str) 
{
	return strmid(str, 0, 6);
}

string cutoff_extension(string str) 
{
	return strxc(str, '.', 0);
}	

string cutoff_tail(string str)
{
	return strx(strx(strx(str, "-M1-NoSession.csv", ""), "-D1-NoSession.csv", ""), ".csv", ""); 
} 

string cutoff_tail_blank(string str) 
{
	return strxc(str, ' ', 0); 
} 

string select_csv() 
{
	return file_select("History","CSV file\0*.csv\0\0");
}

void split(string path) 
{
	char *name;
	char *pch = strtok(path, "\\");
	while (pch != NULL)
   {
		name = pch;
		printf("%s\n", pch);
		pch = strtok(NULL, "\\");
	}
}

char *file_name(string path) 
{
	char *name = 0;
	char *pch = strtok(path, "\\");
	while (pch != NULL)
   {
		name = pch;
		pch = strtok(NULL, "\\");
	}
	return name;
}

function process(string InName, string OutName, string Format)
{
 	printf("\n\n<=== %s", InName);

	int handle = 1;
	char OutNameYear[2048];
	int i;

	dataNew(handle, 0, 0);
	int Records = dataParse(handle, Format, InName);
	printf("\n%d lines read from %s", Records, InName);

#ifdef PRINT_HEAD_TAIL	
		printf("\nHead...\n");
		for(i = 0; i < min(5, Records); i++) {
			printf("\n%s, %f, %f, %f, %f", 
				strdate("%y%m%d %H:%M:%S", dataVar(handle,i,0)), 
				(var)dataVar(handle,i,1), (var)dataVar(handle,i,2), (var)dataVar(handle,i,3), (var)dataVar(handle,i,4)
			);
		}
		printf("\nTail...\n");
		for(i = max(0, Records-5); i < Records; i++) {
			printf("\n%s, %f, %f, %f, %f", 
				strdate("%y%m%d %H:%M:%S", dataVar(handle,i,0)), 
				(var)dataVar(handle,i,1), (var)dataVar(handle,i,2), (var)dataVar(handle,i,3), (var)dataVar(handle,i,4)
			);
		}
		printf("\n");
#endif 

	int Year, Start = 0, LastYear = 0;
	for(i = 0; i < Records; i++) {
		Year = atoi(strdate("%Y", dataVar(1,i,0)));
		if (!LastYear) {
			LastYear = Year;
		}
		if (i == Records-1) { // end of file
			LastYear = Year; 
			Year = 0;
		}
		if(Year != LastYear && LastYear) {
			strcpy(OutNameYear, strf("%s_%4i.t6", OutName, LastYear));
			printf("\n..... %s", OutNameYear);		
			dataSave(handle, OutNameYear, Start, i-Start);
			Start = i;
			LastYear = Year;

#ifdef RELOAD			
			dataNew(handle+1, 0, 0);
			int ReloadedRecords = dataLoad(handle+1, OutNameYear, 7);
			if(!ReloadedRecords) { 
				printf("\nCan't reloead %s", OutNameYear); 
			} else {
				printf("\nReloead %s with %i records", OutNameYear, ReloadedRecords); 
			}	
#endif 
		} 
	}
	printf("\n===> %s\n", OutName);
}

int process_all(string dir, string out, string Format)
{
	char dir_filter[2048];
	char InName[2048];
	char OutName[2048];
	sprintf(dir_filter, "%s\\*.csv", dir);
	char *pch = file_next(dir_filter);
	int n = 0;
	while (pch != NULL)
   {
		sprintf(InName, "%s\\%s", dir, pch);
		sprintf(OutName, "%s\\%s", out, cutoff_tail(pch));
		
		process(InName, OutName, Format);
		pch = file_next(NULL);
		n++;
	}
	return n;
}

int process_only(string dir, string out, string Format, string which, int k)
{
	char dir_filter[2048];
	char InName[2048];
	char OutName[2048];
	sprintf(dir_filter, "%s\\*.csv", dir);
	char *pch = file_next(dir_filter);
	int n = 0;
	while (pch != NULL)
   {
		if (strncmp(pch, which, k) == 0) {	
			sprintf(InName, "%s\\%s", dir, pch);
			sprintf(OutName, "%s\\%s", out, cutoff_tail(pch));
			process(InName, OutName, Format);
			n++;
		} else {
			//printf("\nSkipping %s", pch);
		}
		pch = file_next(NULL);
	}
	return n;
}

void click(int row,int col)
{
	sound("click.wav");
	string Text = panelGet(row,col);
	printf("\nClicked on %i,%i: %s",row,col,Text);
	if(Text == "Ask") 
		panelSet(row,col,"Bid",0,0,0);
	else if(Text == "Bid") 
		panelSet(row,col,"Ask",0,0,0);
	else if(Text == "Run") {
		string in = select_csv();
		string out = cutoff_tail(in);
		process(in, out, Format);
	}
}

function main() 
{
	int n = 0;
	bool Batch = true; 
	bool all = true;
	Verbose = 7;
	
	if(!is(SPONSORED) && Batch == false) {
		printf("Need Zorro S for panel!");
		return;
	}

   // --------------------------------------------------------------
	// Parsing the command line
	// --------------------------------------------------------------
	char in_dir[1024];
	char out_dir[1024];
	
	string param_string = strstr(report(3), "dir=");
	
   string myDir = strmid (param_string, 4, strlen(param_string));
	
	string myOut = strstr(myDir, "out=");
	myOut = strmid (myOut, 4, strlen(myOut));
	
   sscanf (myDir, "%s", myDir);
	
	strcpy(in_dir, myDir);
	strcpy(out_dir, myOut);
   // printf("\nCurrent params: %s # %s", myDir, myOut);
   // --------------------------------------------------------------

	char wd[1024];
	_getcwd(wd, 1024);
	printf("\nCurrent working directory: %s", wd);
	
	if (Batch) 
	{
		string dir = in_dir;
		string out = out_dir;

		if (all) {
			n = process_all(dir, out, Format);
		} else {
			string which = "BTCGBP";
			n = process_only(dir, out, Format, which, 6);
		}
		printf("\n\n=== converted %i data files ===", n);
	} 
	else 
	{	
		panel("Strategy\\CSVtoT6RefinitivPanel.csv", BLACK, 80);
		while(wait(100)) {}
	}
}