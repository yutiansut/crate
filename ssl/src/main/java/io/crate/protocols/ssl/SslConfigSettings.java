/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.protocols.ssl;

import io.crate.settings.CrateSetting;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Settings for configuring Postgres SSL. Only applicable to the ssl-impl module.
 */
public final class SslConfigSettings {

    private SslConfigSettings() {}

    private static final String SSL_HTTP_ENABLED_SETTING_NAME = "ssl.http.enabled";
    private static final String SSL_PSQL_ENABLED_SETTING_NAME = "ssl.psql.enabled";

    static final String SSL_TRUSTSTORE_FILEPATH_SETTING_NAME = "ssl.truststore_filepath";
    static final String SSL_TRUSTSTORE_PASSWORD_SETTING_NAME = "ssl.truststore_password";
    static final String SSL_KEYSTORE_FILEPATH_SETTING_NAME = "ssl.keystore_filepath";
    static final String SSL_KEYSTORE_PASSWORD_SETTING_NAME = "ssl.keystore_password";
    static final String SSL_KEYSTORE_KEY_PASSWORD_SETTING_NAME = "ssl.keystore_key_password";

    private static final String SSL_RESOURCE_POLL_INTERVAL_NAME = "ssl.resource_poll_interval";

    public static final CrateSetting<Boolean> SSL_HTTP_ENABLED = CrateSetting.of(
        Setting.boolSetting(SSL_HTTP_ENABLED_SETTING_NAME, false, Setting.Property.NodeScope),
        DataTypes.BOOLEAN);
    public static final CrateSetting<Boolean> SSL_PSQL_ENABLED = CrateSetting.of(
        Setting.boolSetting(SSL_PSQL_ENABLED_SETTING_NAME, false, Setting.Property.NodeScope),
        DataTypes.BOOLEAN);

    public static final CrateSetting<String> SSL_TRUSTSTORE_FILEPATH = CrateSetting.of(
        Setting.simpleString(SSL_TRUSTSTORE_FILEPATH_SETTING_NAME, Setting.Property.NodeScope),
        DataTypes.STRING);

    public static final CrateSetting<String> SSL_TRUSTSTORE_PASSWORD = CrateSetting.of(
        Setting.simpleString(SSL_TRUSTSTORE_PASSWORD_SETTING_NAME, Setting.Property.NodeScope),
        DataTypes.STRING);

    public static final CrateSetting<String> SSL_KEYSTORE_FILEPATH = CrateSetting.of(
        Setting.simpleString(SSL_KEYSTORE_FILEPATH_SETTING_NAME, Setting.Property.NodeScope),
        DataTypes.STRING);

    public static final CrateSetting<String> SSL_KEYSTORE_PASSWORD = CrateSetting.of(
        Setting.simpleString(SSL_KEYSTORE_PASSWORD_SETTING_NAME, "", Setting.Property.NodeScope),
        DataTypes.STRING);

    public static final CrateSetting<String> SSL_KEYSTORE_KEY_PASSWORD = CrateSetting.of(
        Setting.simpleString(SSL_KEYSTORE_KEY_PASSWORD_SETTING_NAME, Setting.Property.NodeScope),
        DataTypes.STRING);

    public static final CrateSetting<TimeValue> SSL_RESOURCE_POLL_INTERVAL = CrateSetting.of(
        Setting.timeSetting(
            SSL_RESOURCE_POLL_INTERVAL_NAME,
            TimeValue.timeValueSeconds(5),
            Setting.Property.NodeScope),
        DataTypes.STRING);

    public static boolean isHttpsEnabled(Settings settings) {
        return SSL_HTTP_ENABLED.setting().get(settings);
    }

    public static boolean isPSQLSslEnabled(Settings settings) {
        return SSL_PSQL_ENABLED.setting().get(settings);
    }
}
