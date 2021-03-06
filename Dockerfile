# Copyright (c) 2017-2018 Software AG, Darmstadt, Germany and/or its licensors
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this 
# file except in compliance with the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
# either express or implied. 
# See the License for the specific language governing permissions and limitations under the License.

# Create an image from the base Apama image, copying the project in it ready to deploy
ARG APAMA_IMAGE=store/softwareag/apama-correlator:10.5
FROM ${APAMA_IMAGE}

COPY --chown=sagadmin:sagadmin ApamaServerLicense.xml ${APAMA_WORK}/DHLDemo/
COPY --chown=sagadmin:sagadmin init.yaml ${APAMA_WORK}/DHLDemo/
COPY --chown=sagadmin:sagadmin eventdefinitions ${APAMA_WORK}/DHLDemo/eventdefinitions
COPY --chown=sagadmin:sagadmin monitors ${APAMA_WORK}/DHLDemo/monitors
COPY --chown=sagadmin:sagadmin plugin ${APAMA_WORK}/DHLDemo/plugin
COPY --chown=sagadmin:sagadmin lib ${APAMA_WORK}/lib

WORKDIR ${APAMA_WORK}

CMD ["correlator", "--config", "DHLDemo"]
