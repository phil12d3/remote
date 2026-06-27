CXX ?= g++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -pedantic
LDFLAGS ?=
LDLIBS ?= -lssl -lcrypto -pthread

SRC_DIR := src
BUILD_DIR := build

COMMON_SRCS := $(SRC_DIR)/common.cpp $(SRC_DIR)/plan.cpp
COMMON_OBJS := $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(COMMON_SRCS))

all: bin/rc bin/rc-agent

bin:
	mkdir -p bin

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -I$(SRC_DIR) -c $< -o $@

bin/rc: $(BUILD_DIR) $(COMMON_OBJS) $(BUILD_DIR)/controller.o | bin
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(COMMON_OBJS) $(BUILD_DIR)/controller.o $(LDLIBS)

bin/rc-agent: $(BUILD_DIR) $(COMMON_OBJS) $(BUILD_DIR)/agent.o | bin
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(COMMON_OBJS) $(BUILD_DIR)/agent.o $(LDLIBS)

$(BUILD_DIR)/controller.o: $(SRC_DIR)/controller.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -I$(SRC_DIR) -c $< -o $@

$(BUILD_DIR)/agent.o: $(SRC_DIR)/agent.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -I$(SRC_DIR) -c $< -o $@

clean:
	rm -rf $(BUILD_DIR) bin

.PHONY: all clean
