BOT_NAME = "jobs_project"

SPIDER_MODULES = ["jobs_project.spiders"]
NEWSPIDER_MODULE = "jobs_project.spiders"

ROBOTSTXT_OBEY = True

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
