library(tidyverse)
library(stringr)
library(dplyr)
library(lubridate)
library(readr)
library(scales)
library(writexl)
#First we extract the data from Facebook
facebook_posts <- read.csv("Facebook_Post.csv", sep = ",")%>%
                  filter(�..Identificador.de.la.publicación!= '')
facebook_videos <- read.csv("Facebook_Video.csv", sep = ",")%>%
                    filter(�..Identificador.de.la.publicación!= '')
facebook_page <- read.csv("Facebook_Page.csv", sep = ",") %>%
                filter(�..Fecha!= '')

#Second, we select the necessaries variables for our business purpose
facebook_posts <- facebook_posts %>%
                  select(id = �..Identificador.de.la.publicación, 
                            url = Enlace.permanente, 
                            type = Tipo, 
                            date = Publicado, 
                            organic_reach = Lifetime.Post.organic.reach, 
                            paid_reach = Lifetime.Post.Paid.Reach, 
                            organic_impressions = Lifetime.Post.Organic.Impressions, 
                            paid_impressions=Lifetime.Post.Paid.Impressions, 
                            total_impressions=Lifetime.Post.Total.Impressions,
                            engaged_users = Lifetime.Engaged.Users,
                            impressions_by_followers = Lifetime.Post.Impressions.by.people.who.have.liked.your.Page,
                            engagement_by_followers = Lifetime.People.who.have.liked.your.Page.and.engaged.with.your.post,
                            likes = Lifetime.Post.Stories.by.action.type...like,
                            comments = Lifetime.Post.Stories.by.action.type...comment,
                            shares = Lifetime.Post.Stories.by.action.type...share,
                            other_clicks = Lifetime.Post.Audience.Targeting.Unique.Consumptions.by.Type...other.clicks,
                            link_clicks = Lifetime.Post.Audience.Targeting.Unique.Consumptions.by.Type...link.clicks,
                            photo_view = Lifetime.Post.Audience.Targeting.Unique.Consumptions.by.Type...photo.view)

#We adjust the hour in order to have it in Argentina Zone
facebook_posts$date <- mdy_hms(facebook_posts$date)
facebook_posts$date <- facebook_posts$date + 18000
facebook_posts$date <- as.character(facebook_posts$date)
facebook_posts$hour <- str_sub(facebook_posts$date, 12,13)
facebook_posts$date <- str_sub(facebook_posts$date, 1, 10)
facebook_posts$date <- ymd(facebook_posts$date)

#If the value is NULL, it refers to a carrousel content
facebook_posts$type <- ifelse(facebook_posts$type == "", "Carrousel", facebook_posts$type)

#If we don't have interaction in our post, we will see NULL or blanck value. So we have to replace them
facebook_posts$likes <- ifelse(facebook_posts$likes == "", 0, facebook_posts$likes)
facebook_posts$comments<- ifelse(facebook_posts$comments == "", 0, facebook_posts$comments)
facebook_posts$shares<- ifelse(facebook_posts$shares == "", 0, facebook_posts$shares)
facebook_posts$other_clicks<- ifelse(facebook_posts$other_clicks == "", 0, facebook_posts$other_clicks)
facebook_posts$link_clicks<- ifelse(facebook_posts$link_clicks == "", 0, facebook_posts$link_clicks)
facebook_posts$photo_view <- ifelse(facebook_posts$photo_view == "", 0, facebook_posts$photo_view)
facebook_posts$likes[is.na(facebook_posts$likes)] <- 0
facebook_posts$shares[is.na(facebook_posts$shares)] <- 0
facebook_posts$comments[is.na(facebook_posts$comments)] <- 0
facebook_posts$other_clicks[is.na(facebook_posts$other_clicks)] <- 0
facebook_posts$link_clicks[is.na(facebook_posts$link_clicks)] <- 0
facebook_posts$photo_view[is.na(facebook_posts$photo_view)] <- 0

#Cleaning the variables
facebook_posts$paid_reach<- as.numeric(facebook_posts$paid_reach)
facebook_posts$organic_reach <- as.numeric(facebook_posts$organic_reach)
facebook_posts$organic_impressions <- as.numeric(facebook_posts$organic_impressions)
facebook_posts$paid_impressions <- as.numeric(facebook_posts$paid_impressions)
facebook_posts$total_impressions <- as.numeric(facebook_posts$total_impressions)
facebook_posts$engaged_users <- as.numeric(facebook_posts$engaged_users)
facebook_posts$impressions_by_followers <- as.numeric(facebook_posts$impressions_by_followers)
facebook_posts$engagement_by_followers <- as.numeric(facebook_posts$engagement_by_followers)
facebook_posts$likes <- as.numeric(facebook_posts$likes)
facebook_posts$comments <- as.numeric(facebook_posts$comments)
facebook_posts$shares <- as.numeric(facebook_posts$shares)
facebook_posts$other_clicks <- as.numeric(facebook_posts$other_clicks)
facebook_posts$link_clicks <- as.numeric(facebook_posts$link_clicks)
facebook_posts$photo_view <- as.numeric(facebook_posts$photo_view)
facebook_posts$hour <- as.numeric(facebook_posts$hour)

#Creating new variables for business purposes
facebook_posts <- facebook_posts %>%
                  mutate(virality_impressions_rate= (total_impressions-impressions_by_followers)/total_impressions,
                         activity = likes+comments+shares,
                         engagement= likes+comments+shares+other_clicks+photo_view+link_clicks,
                         engagement_rate = engagement/total_impressions,
                         activity_rate = activity/total_impressions,
                         virality_engagement_rate= (engagement-engagement_by_followers)/engagement,
                         CTR= link_clicks/total_impressions)

#Converting as percentage the rates
facebook_posts$virality_impressions_rate <- label_percent()(facebook_posts$virality_impressions_rate)
facebook_posts$virality_engagement_rate <- label_percent()(facebook_posts$virality_engagement_rate)
facebook_posts$engagement_rate <- label_percent()(facebook_posts$engagement_rate)
facebook_posts$activity_rate <- label_percent()(facebook_posts$activity_rate)
facebook_posts$CTR <- label_percent()(facebook_posts$CTR)

#----------------------------------------------------------------------------------------------------
#Selecting the most valuable variables
facebook_page <- facebook_page %>%
                 select(date=1, fanpage_likes=Lifetime.Total.Likes, fanpage_daily_likes = Daily.New.Likes, fanpage_daily_unlikes=Daily.Unlikes,)

#----------------------------------------------------------------------------------------------------
#Now, we use the metrics about videos and clean them
facebook_videos$Reproducciones.totales.hasta.el.95..Total<- as.numeric(facebook_videos$Reproducciones.totales.hasta.el.95..Total)
facebook_videos$Visualizaciones.de.video.totales.Total <- as.numeric(facebook_videos$Visualizaciones.de.video.totales.Total)

#Selecting the most valuable variables for the business case
facebook_videos <- facebook_videos %>%
                  select(id=1, total_views= Visualizaciones.de.video.totales.Total, ninetyfive_percent_of_view= Reproducciones.totales.hasta.el.95..Total)
facebook_videos <- mutate(facebook_videos,  percent_of_retention_at_95 = ninetyfive_percent_of_view/total_views)

#Cleaning the numbers
facebook_videos$percent_of_retention_at_95 <- label_percent()(facebook_videos$percent_of_retention_at_95)

#--------------------------------------------------------------------------------------------------------

#We generate primary keys so we can join them later
facebook_posts$date <- as.character(facebook_posts$date)
facebook_page$date <- as.character(facebook_page$date)

#We join the table about posts and the table about general metrics of the profile
facebook_data <- left_join(facebook_posts, facebook_page)

#We remove the data before January because it is unecessary
facebook_data <- facebook_data %>%
                 filter(date != '2021-02-01')

#We join the metrics about videos so we hae everything in one big dataframe
facebook_data <- left_join(facebook_data, facebook_videos)
facebook_data$total_views[is.na(facebook_data$total_views)] <- 0
facebook_data$percent_of_retention_at_95[is.na(facebook_data$percent_of_retention_at_95)] <- 0
facebook_data <- select(facebook_data, -ninetyfive_percent_of_view)

#We export the final dataframe to a Excel file
write_xlsx(facebook_data,"./Facebook.xlsx")
