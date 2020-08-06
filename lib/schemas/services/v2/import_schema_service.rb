# frozen_string_literal: true

require 'zip'

module Schemas
  module Services
    module V2
      class ImportSchemaService
        attr_reader :es, :hashlink_generator

        def initialize(es, hashlink_generator)
          @es = es
          @hashlink_generator = hashlink_generator
        end

        def call(namespace, raw_params)
          raise "Namespace '_any' is forbidden" if namespace == '_any'

          params = validate(raw_params)
          file = params[:file][:tempfile]
          type = params[:file][:filename].split('.').last
          schema_base_validator = SchemaBaseValidator.new(
            schema_name_validator: SchemaNameValidator.new(es, namespace)
          )
          if type == 'json'
            schema_base = JSON.parse(file.read)
            result = schema_base_validator.call(schema_base)
            raise result.errors.to_h if result.failure?
            dri = store_schema_base(namespace, schema_base)
          elsif type == 'zip'
            schema = extract_zip(file)
            result = schema_base_validator.call(schema[:schema_base].values[0])
            raise result.errors.to_h if result.failure?
            dri = store_branch(namespace, schema)
          else
            raise 'File type must be json or zip'
          end
        end

        def validate(params)
          return unless params['file']
          {
            file: params['file']
          }
        end

        private def store_schema_base(namespace, schema_base)
          hashlink = hashlink_generator.call(schema_base)
          record = {
            _id: namespace + '/' + hashlink,
            namespace: namespace,
            DRI: hashlink,
            data: schema_base
          }
          es.index(:schema_base).bulk_index([record])
          hashlink
        end

        private def store_branch(namespace, schema)
          es.index(:schema_base).bulk_index(
            schema[:schema_base].map do |hashlink, content|
              {
                _id: namespace + '/' + hashlink,
                namespace: namespace,
                DRI: hashlink,
                data: content,
                'name-suggest' => [namespace, content['name']]
              }
            end
          )
          es.index(:overlay).bulk_index(
            schema[:overlay].map do |hashlink, content|
              {
                _id: namespace + '/' + hashlink,
                namespace: namespace,
                DRI: hashlink,
                data: content
              }
            end
          )

          branch = {
            schema_base: schema[:schema_base].keys.first,
            overlays: schema[:overlay].keys.sort
          }
          branch_hashlink = hashlink_generator.call(branch)
          branch_record = {
            _id: namespace + '/' + branch_hashlink,
            namespace: namespace,
            DRI: branch_hashlink,
            data: branch
          }
          es.index(:branch).bulk_index([branch_record])
          branch_hashlink
        end

        private def extract_zip(file)
          schema = { overlay: {} }
          Zip::File.open(file) do |zip|
            zip.each do |entry|
              next unless entry.ftype == :file
              content = JSON.parse(entry.get_input_stream.read)
              hashlink = hashlink_generator.call(content)
              type = entry.name.split('/').size == 1 ? :schema_base : :overlay
              if type == :schema_base
                schema[:schema_base] = { hashlink => content }
              elsif type == :overlay
                schema[:overlay].merge!(hashlink => content)
              end
            end
          end
          schema
        end

        class SchemaBaseValidator < Dry::Validation::Contract
          option :schema_name_validator

          params do
            required(:@context).filled(:string)
            required(:name).filled(:string)
            required(:type).filled(:string)
            required(:description).value(:str?)
            required(:classification).value(:str?)
            required(:issued_by).value(:str?)
            required(:attributes).filled(:hash)
            required(:pii_attributes).value(:array?)
          end

          rule(:name) do
            unless schema_name_validator.available?(values[:name])
              key.failure('Schema Base name is taken in that namespace')
            end
          end
        end

        class SchemaNameValidator
          attr_reader :es, :namespace

          def initialize(es, namespace)
            @es = es
            @namespace = namespace
          end

          def available?(name)
            query = {
              bool: {
                must:  [
                  { match: { namespace: namespace } },
                  { match: { 'data.name' => name } }
                ]
              }
            }
            total = es.index(:schema_base)
              .search(size: 1, query: query)
              .total['value']
            total.zero?
          end
        end
      end
    end
  end
end
