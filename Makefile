BUILD_DIR := $(CURDIR)/_build

REBAR := rebar3

CT_NODE_NAME = ct@127.0.0.1

.PHONY: all
all: compile

compile:
	$(REBAR) do compile, xref, dialyzer

.PHONY: clean
clean: distclean

.PHONY: distclean
distclean:
	@rm -rf _build erl_crash.dump rebar3.crashdump rebar.lock

.PHONY: xref
xref:
	$(REBAR) xref

.PHONY: eunit
eunit: compile
	$(REBAR) eunit verbose=true

.PHONY: ct
ct: compile
	$(REBAR) do eunit, ct -v --readable=false --name $(CT_NODE_NAME)

.PHONY: ct-suite
ct-suite: compile
ifneq ($(TESTCASE),)
	$(REBAR) ct -v --readable=false --name $(CT_NODE_NAME) --suite $(SUITE)  --case $(TESTCASE)
else
	$(REBAR) ct -v --readable=false --name $(CT_NODE_NAME) --suite $(SUITE)
endif

cover:
	$(REBAR) cover

.PHONY: coveralls
coveralls:
	@rebar3 as test coveralls send

.PHONY: dialyzer
dialyzer:
	$(REBAR) dialyzer

CUTTLEFISH_SCRIPT = _build/default/lib/cuttlefish/cuttlefish

$(CUTTLEFISH_SCRIPT):
	@${REBAR} get-deps
	@if [ ! -f cuttlefish ]; then make -C _build/default/lib/cuttlefish; fi

app.config: $(CUTTLEFISH_SCRIPT)
	$(verbose) $(CUTTLEFISH_SCRIPT) -l info -e etc/ -c etc/ekka.conf.example -i priv/ekka.schema -d data/
