# -*- coding: utf-8 -*-
import datetime
from south.db import db
from south.v2 import SchemaMigration
from django.db import models


class Migration(SchemaMigration):

    def forwards(self, orm):
        # Adding field 'Newsletter.private'
        db.add_column(u'news_newsletter', 'private',
                      self.gf('django.db.models.fields.BooleanField')(default=False),
                      keep_default=False)


    def backwards(self, orm):
        # Deleting field 'Newsletter.private'
        db.delete_column(u'news_newsletter', 'private')


    models = {
        u'news.apiuser': {
            'Meta': {'object_name': 'APIUser'},
            'api_key': ('django.db.models.fields.CharField', [], {'default': "'ea007af5-4292-4fcd-bc69-01f45c8a09ed'", 'max_length': '40', 'db_index': 'True'}),
            'enabled': ('django.db.models.fields.BooleanField', [], {'default': 'True'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'name': ('django.db.models.fields.CharField', [], {'max_length': '256'})
        },
        u'news.blockedemail': {
            'Meta': {'object_name': 'BlockedEmail'},
            'email_domain': ('django.db.models.fields.CharField', [], {'max_length': '50'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'})
        },
        u'news.failedtask': {
            'Meta': {'object_name': 'FailedTask'},
            'args': ('jsonfield.fields.JSONField', [], {'default': '[]'}),
            'einfo': ('django.db.models.fields.TextField', [], {'default': 'None', 'null': 'True'}),
            'exc': ('django.db.models.fields.TextField', [], {'default': 'None', 'null': 'True'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'kwargs': ('jsonfield.fields.JSONField', [], {'default': '{}'}),
            'name': ('django.db.models.fields.CharField', [], {'max_length': '255'}),
            'task_id': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': '255'}),
            'when': ('django.db.models.fields.DateTimeField', [], {'default': 'datetime.datetime.now'})
        },
        u'news.interest': {
            'Meta': {'object_name': 'Interest'},
            '_welcome_id': ('django.db.models.fields.CharField', [], {'max_length': '64', 'blank': 'True'}),
            'default_steward_emails': ('news.fields.CommaSeparatedEmailField', [], {'blank': 'True'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'interest_id': ('django.db.models.fields.SlugField', [], {'unique': 'True', 'max_length': '50'}),
            'title': ('django.db.models.fields.CharField', [], {'max_length': '128'})
        },
        u'news.localestewards': {
            'Meta': {'unique_together': "(('interest', 'locale'),)", 'object_name': 'LocaleStewards'},
            'emails': ('news.fields.CommaSeparatedEmailField', [], {}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'interest': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['news.Interest']"}),
            'locale': ('news.fields.LocaleField', [], {'max_length': '32'})
        },
        u'news.newsletter': {
            'Meta': {'ordering': "['order']", 'object_name': 'Newsletter'},
            'active': ('django.db.models.fields.BooleanField', [], {'default': 'True'}),
            'confirm_message': ('django.db.models.fields.CharField', [], {'max_length': '64', 'blank': 'True'}),
            'description': ('django.db.models.fields.CharField', [], {'max_length': '256', 'blank': 'True'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'languages': ('django.db.models.fields.CharField', [], {'max_length': '200'}),
            'order': ('django.db.models.fields.IntegerField', [], {'default': '0'}),
            'private': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'requires_double_optin': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'show': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'slug': ('django.db.models.fields.SlugField', [], {'unique': 'True', 'max_length': '50'}),
            'title': ('django.db.models.fields.CharField', [], {'max_length': '128'}),
            'vendor_id': ('django.db.models.fields.CharField', [], {'max_length': '128'}),
            'welcome': ('django.db.models.fields.CharField', [], {'max_length': '64', 'blank': 'True'})
        },
        u'news.newslettergroup': {
            'Meta': {'object_name': 'NewsletterGroup'},
            'active': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'description': ('django.db.models.fields.CharField', [], {'max_length': '256', 'blank': 'True'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'newsletters': ('django.db.models.fields.related.ManyToManyField', [], {'related_name': "'newsletter_groups'", 'symmetrical': 'False', 'to': u"orm['news.Newsletter']"}),
            'show': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'slug': ('django.db.models.fields.SlugField', [], {'unique': 'True', 'max_length': '50'}),
            'title': ('django.db.models.fields.CharField', [], {'max_length': '128'})
        },
        u'news.smsmessage': {
            'Meta': {'object_name': 'SMSMessage'},
            'description': ('django.db.models.fields.CharField', [], {'max_length': '200', 'blank': 'True'}),
            'message_id': ('django.db.models.fields.SlugField', [], {'max_length': '50', 'primary_key': 'True'}),
            'vendor_id': ('django.db.models.fields.CharField', [], {'max_length': '50'})
        },
        u'news.subscriber': {
            'Meta': {'object_name': 'Subscriber'},
            'email': ('django.db.models.fields.EmailField', [], {'max_length': '75', 'primary_key': 'True'}),
            'fxa_id': ('django.db.models.fields.CharField', [], {'db_index': 'True', 'max_length': '100', 'null': 'True', 'blank': 'True'}),
            'token': ('django.db.models.fields.CharField', [], {'default': "'f92b5b31-dc5f-44f3-8d3e-e91fe94fb05a'", 'max_length': '40', 'db_index': 'True'})
        }
    }

    complete_apps = ['news']