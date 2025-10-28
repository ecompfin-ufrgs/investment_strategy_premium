
library(arrow)
library(tibble)

# Function
yieldsbr = function(Initial_Date,Final_Date,Maturities){
  # Packages
  packages = c("rvest","httr","functional")
  new.packages = packages[!(packages %in% installed.packages()[,"Package"])]
  if(length(new.packages)) install.packages(new.packages)
  suppressMessages(library(rvest))
  suppressMessages(library(httr))
  suppressMessages(library(functional))
  
  dates = format(seq(as.Date(Initial_Date), as.Date(Final_Date), 'day'), format="%d-%m-%Y", tz="UTC")
  mat = matrix(NA,length(dates),length(Maturities))
  # Scraping
  for(i in 1:length(dates)){
    di = GET(url = "https://www2.bmf.com.br/pages/portal/bmfbovespa/lumis/lum-taxas-referenciais-bmf-ptBR.asp",query = list(Data = dates[i]))
    data = read_html(di) %>% html_nodes("table") %>% html_nodes("td") %>% html_text()
    if(length(data)==0){i=i
    }else{
      data = data.frame(matrix(data, ncol=3, byrow=TRUE))
      data[,2] = as.numeric(gsub(",", ".", gsub("\\.", "", data[,2])))
      data[,3] = as.numeric(gsub(",", ".", gsub("\\.", "", data[,3])))
      # Spline
      t = as.integer(as.matrix(data[,1]))/21
      y = as.numeric(as.matrix(data[,2]))
      spl = smooth.spline(y ~ t, spar=.001)
      t.new = Maturities
      new = predict(spl, t.new)
      mat[i,] = new$y
      pb = txtProgressBar(min = (1/length(dates)), max = length(dates), style = 3)
      setTxtProgressBar(pb,i)
    }
  }
  colnames(mat) = paste0("M",Maturities)
  rownames(mat) = dates
  mat = mat[apply(mat, 1, Compose(is.finite, all)),]
  return(mat)
}

# Example
Initial_Date = '2004/01/01' # Available from 2003/08/08. YYYY/MM/DD 
Final_Date = '2024/12/31'
Maturities <- seq(1, 120, by = 1)

yields = yieldsbr(Initial_Date=Initial_Date,Final_Date=Final_Date,Maturities=Maturities)
yields



# Converter para data frame
yields_df <- as.data.frame(yields)
yields_df <- rownames_to_column(yields_df, var = "Date")

# Escrever como parquet
write_parquet(yields_df, "C:/Users/Bernardo Machado/OneDrive/Área de Trabalho/TCC/data_colection/curvas_b3.parquet")

