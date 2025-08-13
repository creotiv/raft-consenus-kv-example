.PHONY: run1
run1:
	CONFIG_PATH=configs/node1.yaml uvicorn main:create_app --factory --port 7101

.PHONY: run2
run2:
	CONFIG_PATH=configs/node2.yaml uvicorn main:create_app --factory --port 7102

.PHONY: run3
run3:
	CONFIG_PATH=configs/node3.yaml uvicorn main:create_app --factory --port 7103

.PHONY: run-all
run-all:
	CONFIG_PATH=configs/node1.yaml uvicorn main:create_app --factory --port 7101 &
	CONFIG_PATH=configs/node2.yaml uvicorn main:create_app --factory --port 7102 &
	CONFIG_PATH=configs/node3.yaml uvicorn main:create_app --factory --port 7103 &
	wait

.PHONY: test-k6
test-k6:
	k6 run k6-bench.js