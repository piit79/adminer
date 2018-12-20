NAME=asm-adminer
VERSION=$(shell grep '$VERSION' adminer/include/version.inc.php | cut -d'"' -f 2)
VERSION_SUFFIX=-ca1
VERSION_FULL=$(VERSION)$(VERSION_SUFFIX)
ADMINER_FILE=adminer-$(VERSION).php
INDEX_LINK=index.php
ADMINER_URL=https://www.adminer.org/static/download/$(VERSION)/$(ADMINER_FILE)
ADMINER_CSS=adminer.css
ADMINER_CSS_URL=https://raw.githubusercontent.com/vrana/adminer/master/designs/pokorny/adminer.css

.PHONY: all

all: deb

$(ADMINER_FILE):
	./compile.php
	mv adminer.php $(ADMINER_FILE)

.PHONY: $(ADMINER_LINK)
$(INDEX_LINK): $(ADMINER_FILE)
	ln -sf $(ADMINER_FILE) $(INDEX_LINK)

.PHONY: deb
deb: $(ADMINER_FILE) $(INDEX_LINK)
	fpm -f -s dir -t deb -n $(NAME) -v $(VERSION_FULL) -a all \
		--description "Adminer - database management in a single PHP file" \
		--maintainer "CA ASM Team <team-asm-notification@ca.com>" \
		--vendor "Jakub Vrana" \
		--url https://www.adminer.org/ \
		--license Apache \
		./$(ADMINER_FILE)=/fs0/asm/adminer/ \
		./adminer/index2.php=/fs0/asm/adminer/ \
		./adminer/config.inc.php.sample=/fs0/asm/adminer/ \
		./designs/pokorny/$(ADMINER_CSS)=/fs0/asm/adminer/ \
		./$(INDEX_LINK)=/fs0/asm/adminer/

.PHONY: clean
clean:
	rm -f $(ADMINER_FILE) $(INDEX_LINK) *.deb
