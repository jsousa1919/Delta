import StockPickr 
import StockTwits 
import StockHouse
import GTrends
import FlameIndex
import SocialPicks
import Text
import Task

TaskTypes = [   StockPickr.SPQueryUserTask,
                StockPickr.SPQueryPortfolioTask,
                StockPickr.SPQueryArticleTask,
                StockPickr.SPScrapeUpdatesTask,
                StockPickr.SPDumpTask,
                StockTwits.STTask,
                StockTwits.STScrapeTask,
                StockTwits.STQueryUserTask,
                StockTwits.STQueryStockTask,
                StockTwits.STQueryTrendingNowTask,
                StockTwits.STDumpTask,
                StockHouse.SHTask,
                StockHouse.SHScrapeTask,
                StockHouse.SHQueryStock,
                StockHouse.SHDumpTask,
                StockHouse.SHQueryArticle,
                GTrends.GTScrapeTask,
                GTrends.GTQueryStockTask,
                GTrends.GTDumpTask,
                FlameIndex.FIScrapeTask,
                FlameIndex.FIQueryStockTask,
                FlameIndex.FIDumpTask,
                SocialPicks.SoPiScrapeTask,
                SocialPicks.SoPiQueryStockTask,
                SocialPicks.SoPiDumpTask,
                Text.TextAnalysisTask,
                Task.DieTask
            ]
