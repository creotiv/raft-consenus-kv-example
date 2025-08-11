.PHONY: run1
run1:
	CONFIG_PATH=configs/node1.yaml uvicorn main:create_app --factory --port 7101

.PHONY: run2
run2:
	CONFIG_PATH=configs/node2.yaml uvicorn main:create_app --factory --port 7102

.PHONY: run3
run3:
	CONFIG_PATH=configs/node3.yaml uvicorn main:create_app --factory --port 7103
