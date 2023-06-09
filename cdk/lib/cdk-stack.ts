import * as cdk from 'aws-cdk-lib';
import { BillingMode } from 'aws-cdk-lib/aws-dynamodb';
import { RustFunction } from 'cargo-lambda-cdk';
import { Construct } from 'constructs';

export class CdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const stage = this.node.tryGetContext('stage');

    //DynamoDB table
    // 郵便番号テーブルは、パーティションキーを郵便番号とする
    const postalCodes = new cdk.aws_dynamodb.Table(this, 'PostalCodes', {
      tableName: `postal-codes-${stage}`,
      partitionKey: {
        type: cdk.aws_dynamodb.AttributeType.STRING,
        name: 'postal_code',
      },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    //ハッシュ値保存用のテーブル
    const hashTable = new cdk.aws_dynamodb.Table(this, 'HashTable', {
      tableName: `hash-table-${stage}`,
      partitionKey: {
        type: cdk.aws_dynamodb.AttributeType.STRING,
        name: 'id',
      },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    //role for lambda
    const role = new cdk.aws_iam.Role(this, 'RustLambdaRole', {
      roleName: `rust-lambda-role-${stage}`,
      assumedBy: new cdk.aws_iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [cdk.aws_iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')],
      inlinePolicies: {
        UserTablePut: new cdk.aws_iam.PolicyDocument({
          statements: [new cdk.aws_iam.PolicyStatement({
            actions: ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:BatchWriteItem'],
            effect: cdk.aws_iam.Effect.ALLOW,
            resources: [
              `arn:aws:dynamodb:${this.region}:${this.account}:table/postal-codes-*`,
              `arn:aws:dynamodb:${this.region}:${this.account}:table/hash-table-*`,
            ]
          })]
        })
      }
    });

    // Lambda function

    //郵便番号を取得するLambda
    const getPostalCodeLambda = new RustFunction(this, 'postal-code', {
      manifestPath: '../get-postal-code/Cargo.toml',
      functionName: `get-postal-code-${stage}`,
      description: "Get PostalCode Information from DynamoDB",
      environment: {
        POSTAL_CODE_TABLE: postalCodes.tableName,
      },
      role: role,
    });

    //郵便番号を更新するLambda
    const updatePostalCodeLambda = new RustFunction(this, 'update-postal-code', {
      manifestPath: '../update-postal-code/Cargo.toml',
      functionName: `update-postal-code-${stage}`,
      description: "Update PostalCode Information to DynamoDB",
      environment: {
        POSTAL_CODE_TABLE: postalCodes.tableName,
        HASH_TABLE: hashTable.tableName,
      },
      timeout: cdk.Duration.minutes(10),
      memorySize: 512,
      role: role,
    });

    //API Gateway
    const api = new cdk.aws_apigateway.RestApi(this, 'RustLambdaApi', {
      deployOptions: {
        stageName: stage,
      }
    });
    //POST: /postal-code
    api.root
      .addResource('postal-code')
      .addResource('{postalCode}')
      .addMethod('GET', new cdk.aws_apigateway.LambdaIntegration(getPostalCodeLambda), {
        requestParameters: {
          'method.request.path.postalCode': true,
        },
        requestValidator: api.addRequestValidator('postal-code-validator', {
          validateRequestParameters: true,
        })
      });

    // CFn Outputs
    //new cdk.CfnOutput(this, 'ApiEndpoint', {
    //  value: api.urlForPath('/postal-code')
    //});
  }
}
