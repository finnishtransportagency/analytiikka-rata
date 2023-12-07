# analytiikka-rata



Hakemistorakenne:

stack/

analytiikka_stack.py  cicd stack
analytiikka_stage.py  pipeline staget (= dev/prod)
analytiikka_services_stack.py  stage sisältämät palvelut. Uudet asiat lisätään tänne
helper_glue.py  Apukoodi Glue- ajojen luontiin
helper_lambda.py  Apukoodi Lambdojen luontiin
helper_parameter.py  Apukoodi resurssien ympäristökohtaisten parametrien käyttöön
helper_tags.py  Apukoodi Tagien lisäykseen

lambda/xxx/  Jokaiselle lambdalle oma hakemisto. Jos python, hakemistossa pitää olla requirements.txt mutta se voi olla tyhjä jos ei tarvita. Testattu python, Java, node. Layerit eii testattu, lisäkirjastot ei testattu. Vpc ok.
lambda/testi1/  Python testi
lambda/servicenow/  Servicenow api lukija, Java

glue/xxx/  Jokaiselle glue- jobille oma hakemisto. Python shell ja spark testattu. Connectin luonti ok, iimport eii testattu.

lambda/xxx/ ja glue/xxx/ hakemistoihin voi luoda xxx_parameters.json nimisen tiedoston josta luetaan helposti kyseisen lambdan/jobin ympäristökohtaiset parametrit.
Katso lisätiiedot stack/helper_parameter.py



Asennus:

Profiileihin kopioitu väyläpilven tilapäiset kredentiaalit

Secret github- yhteyttä varten
aws secretsmanager create-secret --name analytiikka-github-token --secret-string <github token> --profile dev_LatausalueAdmin

Tuotantotili parametrista
aws ssm put-parameter --name analytiikka-prod-account --value <prod account id> --profile dev_LatausalueAdmin
aws ssm put-parameter --name analytiikka-prod-account --value <prod account id> --profile prod_LatausalueAdmin

Bootstrap kerran molemmille tileille
npx cdk bootstrap aws://DEV-ACCOUNT-ID/eu-west-1 --cloudformation-execution-policies "arn:aws:iam::aws:policy/AdministratorAccess" --profile dev_LatausalueAdmin

npx cdk bootstrap aws://PROD-ACCOUNT-ID/eu-west-1 --trust DEV-ACCOUNT-ID --cloudformation-execution-policies "arn:aws:iam::aws:policy/AdministratorAccess" --profile prod_LatausalueAdmin

git commit &  push

npx cdk deploy --profile dev_LatausalueAdmin



HUOM: cdk synth luo cdk.context.json tiedoston jota ei tallenneta gittiin.


Normaali käyttö:

git push master- branshiin käynnistää pipelinen joka asentaa kaiken.



https://github.com/aws-samples/aws-cdk-examples/tree/master/python

