# frozen_string_literal: true

require "mysql2"
require "reverse_markdown"
require File.expand_path(File.dirname(__FILE__) + "/base.rb")

class ImportScripts::Flarum < ImportScripts::Base
  FLARUM_HOST = ENV["FLARUM_HOST"] || "localhost"
  FLARUM_DB = ENV["FLARUM_DB"] || "amarador_ke_community"
  FLARUM_USER = ENV["FLARUM_USER"] || "root"
  FLARUM_PW = ENV["FLARUM_PW"] || "Qwerty@1234"
  FLARUM_TABLE_PREFIX = ENV["TABLE_PREFIX"] || "com"
  FLARUM_UPLOADS_DIR = "/Users/apple/Downloads/avatars"
  
  BATCH_SIZE = 5000

  SiteSetting.disable_emails = "non-staff"
  SiteSetting.disable_digest_emails = true
  
  def initialize
    super

    @client =
      Mysql2::Client.new(
        host: FLARUM_HOST,
        username: FLARUM_USER,
        password: FLARUM_PW,
        database: FLARUM_DB,
        encoding: "utf8mb4"
      )
  end

  def execute
    import_users
    import_categories
    import_topics_and_posts
    import_likes
    import_reactions
    add_users_to_groups
    mark_topic_as_closed
  end

  def import_users
    puts "", "importing users..."

    total_users = mysql_query("SELECT COUNT(*) count FROM #{FLARUM_TABLE_PREFIX}users").first["count"]
    
    batches(BATCH_SIZE) do |offset|
      users = mysql_query(<<~SQL).to_a
        SELECT id
              , username
              , nickname
              , email
              , joined_at
              , last_seen_at
              , is_email_confirmed
              , suspended_until
              , bio
              , avatar_url
        FROM #{FLARUM_TABLE_PREFIX}users
        ORDER BY id
        LIMIT #{BATCH_SIZE}
        OFFSET #{offset}
      SQL
        
      break if users.empty?

      create_users(users, total: total_users, offset: offset) do |user|
        next if user_id_from_imported_user_id(user["id"])
        next if user["username"].blank? || !Email.is_valid?(user["email"])
        {
          id: user["id"],
          username: user["username"],
          name: user["nickname"] || user["username"],
          email: user["email"],
          created_at: user["joined_at"],
          last_seen_at: user["last_seen_at"],
          suspended_till: user["suspended_until"],
          bio_raw: user["bio"],
          approved: user["is_email_confirmed"],
          post_create_action:
            proc do |new_user|
              if user["avatar_url"]
                avatar_file_name = user["avatar_url"]
                path = File.join(FLARUM_UPLOADS_DIR, avatar_file_name)
                if File.exist?(path)
                  begin
                    upload = create_upload(new_user.id, path, File.basename(path))
                    if upload && upload.persisted?
                      new_user.create_user_avatar
                      new_user.user_avatar.update(custom_upload_id: upload.id)
                      new_user.update(uploaded_avatar_id: upload.id)
                    end
                  rescue StandardError => e
                    puts "Failed to upload avatar for user #{u["id"]}: #{e.message}"
                  end
                end
              end
            end
        }
      end
    end
  end

  def import_categories
    puts "", "importing categories and subcategories..."
    
    categories = mysql_query(<<~SQL).to_a
      SELECT id
            , name
            , slug
            , position
            , description
            , color
            , parent_id
            , position
            , is_restricted
            , discussion_count
            , icon
            , created_at
            , updated_at
      FROM #{FLARUM_TABLE_PREFIX}tags
      ORDER BY id
    SQL

    return if categories.empty?

    parent_categories = categories.select { |c| c["parent_id"].nil? }
    child_categories = categories.select { |c| !c["parent_id"].nil? }
    
    create_categories(parent_categories) do |c|
      next if category_id_from_imported_category_id(c["id"])
      {
        id: c["id"],
        name: c["name"].encode("utf-8", "utf-8"),
        position: c["position"],
        description: c["description"],
        color: c["color"] ? c["color"].gsub('#', '')[0, 6] : nil,
        read_restricted: c["is_restricted"],
        icon: c["icon"],
        created_at: c["created_at"],
        updated_at: c["updated_at"]
      }
    end
      
    create_categories(child_categories) do |c|
      next if category_id_from_imported_category_id(c["id"])
      {
        id: c["id"],
        parent_category_id: category_id_from_imported_category_id(c["parent_id"]),
        name: c["name"].encode("utf-8", "utf-8"),
        position: c["position"],
        description: c["description"],
        color: c["color"] ? c["color"].gsub('#', '')[0, 6] : nil,
        read_restricted: c["is_restricted"],
        icon: c["icon"],
        created_at: c["created_at"],
        updated_at: c["updated_at"]
      }
    end
  end
  
  def import_topics_and_posts
    puts "", "importing topics and posts..."

    total_count = mysql_query(<<~SQL).to_a
      SELECT COUNT(*) AS count
      FROM #{FLARUM_TABLE_PREFIX}discussions d
        LEFT JOIN #{FLARUM_TABLE_PREFIX}posts p ON p.discussion_id = d.id
        LEFT JOIN #{FLARUM_TABLE_PREFIX}discussion_tag t ON t.discussion_id = d.id
      WHERE p.type = 'comment' AND d.hidden_at IS NULL AND p.hidden_at IS NULL
    SQL
    .first["count"]

    guest_user = ensure_guest_user

    batches(BATCH_SIZE) do |offset|
      results = mysql_query(<<~SQL).to_a
        SELECT p.id AS id
          , d.id AS topic_id
          , d.title AS title
          , d.first_post_id AS first_post_id
          , p.user_id AS user_id
          , p.content AS raw
          , p.created_at AS created_at
          , t.tag_id AS category_id
          , d.is_sticky AS pinned_globally
          , d.is_locked AS closed
          , d.is_private AS visible
          , p.hidden_at AS hidden_at
          , d.slug AS slug
          , p.number AS post_number
        FROM #{FLARUM_TABLE_PREFIX}discussions d
        LEFT JOIN #{FLARUM_TABLE_PREFIX}posts p ON p.discussion_id = d.id
        LEFT JOIN #{FLARUM_TABLE_PREFIX}discussion_tag t ON t.discussion_id = d.id
        WHERE p.type = 'comment' AND d.hidden_at IS NULL AND p.hidden_at IS NULL
        ORDER BY p.created_at
        LIMIT #{BATCH_SIZE}
        OFFSET #{offset};
      SQL

      break if results.empty?

      results.group_by { |r| r["topic_id"] }.each do |topic_id, posts|
        available_post_ids = posts.map { |p| p["id"] }
        min_post = posts.min_by { |p| p["id"].to_i }

        posts.each do |p|
          current_first_id = p["first_post_id"]

          if current_first_id.nil?
            p["first_post_id"] = min_post["id"]
          elsif !available_post_ids.include?(current_first_id)
            fallback_post = posts.min_by { |x| x["id"].to_i }
            p["first_post_id"] = fallback_post["id"]
          end
        end
      end

      create_posts(results, total: total_count, offset: offset) do |m|
        skip = false
        mapped = {}

        if mention_match = m["raw"].match(/<POSTMENTION\s+[^>]*discussionid="#{m["topic_id"]}"[^>]*number="(\d+)"[^>]*>/i)
          target_postnum = mention_match[1].to_i
          next unless target_postnum && target_postnum > 0
          mapped[:reply_to_post_number] = target_postnum
        end

        mapped[:id] = m["id"]
        mapped[:user_id] = user_id_from_imported_user_id(m["user_id"]) || guest_user.id
        mapped[:raw] = clean_up(m["raw"])
        mapped[:created_at] = Time.zone.at(m["created_at"])

        if m["id"] == m["first_post_id"]
          mapped[:category] = category_id_from_imported_category_id(m["category_id"])
          mapped[:title] = CGI.unescapeHTML(m["title"])
          if m["pinned_globally"].to_i == 1
            mapped[:pinned_globally] = true
            mapped[:pinned_at] = m["created_at"]
          end
          if m["visible"].to_i == 1
            mapped[:visible] = false
          end
          mapped[:post_create_action] = proc do |pp|
            Permalink.find_or_create_by(url: "d/#{m["topic_id"]}-#{m["slug"]}", topic_id: pp.topic_id)
          end
        else
          parent = topic_lookup_from_imported_post_id(m["first_post_id"])
          if parent
            mapped[:topic_id] = parent[:topic_id]
            mapped[:post_number] = m["post_number"]
            mapped[:post_create_action] = proc do |pp|
              Permalink.find_or_create_by(url: "d/#{m["topic_id"]}-#{m["slug"]}/#{m["post_number"]}", post_id: pp.id)
            end
          else
            puts "Parent post #{m["first_post_id"]} doesn't exist. Skipping #{m["id"]}: #{m["title"]}"
            skip = true
          end
        end
        skip ? nil : mapped
      end
    end
  end

  def import_likes
    puts "", "importing likes..."

    SiteSetting.max_likes_per_day = 5000000
    
    likes = mysql_query(<<~SQL).to_a
      SELECT post_id
            , user_id
            , created_at
      FROM #{FLARUM_TABLE_PREFIX}post_likes
    SQL

    return if likes.empty?

    likes.each do |like|
      user_id = user_id_from_imported_user_id(like["user_id"])
      next unless user_id

      post_id = post_id_from_imported_post_id(like["post_id"])
      next unless post_id

      user = User.find_by(id: user_id)
      next unless user

      post = Post.find_by(id: post_id)
      next unless post

      next if PostAction.exists?(user_id: user_id, post_id: post_id, post_action_type_id: 2, created_at: like[:created_at])

      begin
        PostActionCreator.like(user,post)

      rescue StandardError => e
        puts "Error importing like for post #{like["post_id"]} by user #{like["user_id"]}: #{e.message}"
      end
    end

    SiteSetting.max_likes_per_day = 50
  end

  def import_reactions
    puts "", "importing reactions..."

    RateLimiter.disable
    
    reactions = mysql_query(<<~SQL).to_a
      SELECT user_id
            , post_id
            , identifier
      FROM #{FLARUM_TABLE_PREFIX}reactions cr JOIN #{FLARUM_TABLE_PREFIX}post_reactions cpr
      ON cr.id = cpr.id;
    SQL

    return if reactions.empty?

    reactions.each do |reaction|
      user_id = user_id_from_imported_user_id(reaction["user_id"])
      next unless user_id

      post_id = post_id_from_imported_post_id(reaction["post_id"])
      next unless post_id

      user = User.find_by(id: user_id)
      next unless user

      post = Post.find_by(id: post_id)
      next unless post

      begin
        reaction_manager = DiscourseReactions::ReactionManager.new(
          reaction_value: reaction["identifier"],
          user: user,
          post: post
        )
        reaction_manager.toggle!
      rescue Discourse::InvalidAccess => e
        puts "Skipping reaction: Invalid access for user #{user.id}, post #{post.id}. #{e.message}"
        next
      end
    end
    RateLimiter.enable
  end

  def add_users_to_groups
    puts "", "importing group users"

    group_name_map = {
      "Admin" => "admins",
      "Moderator" => "moderators"
    }

    group_users = mysql_query(<<-SQL).to_a
      SELECT cgu.user_id AS user_id
            , cg.name_singular AS source_group_name
      FROM #{FLARUM_TABLE_PREFIX}group_user cgu
      JOIN #{FLARUM_TABLE_PREFIX}groups cg ON cgu.group_id = cg.id;
    SQL

    group_users.each do |row|
      discourse_group_name = group_name_map[row["source_group_name"]]
      next unless discourse_group_name
      user = User.find_by(id: user_id_from_imported_user_id(row["user_id"]))
      group = Group.find_by(name: discourse_group_name)
      next unless user && group

      GroupUser.find_or_create_by(user_id: user.id, group_id: group.id)
    end
  end
  
  def ensure_guest_user
    guest = User.find_by_username('guest')
    unless guest
      guest = User.create!(
        username: 'Guest',
        email: 'guest@kleinanzeigen.de',
        name: 'Guest User',
      )
      guest.activate
      guest.save!
    end
    guest
  end

  def mark_topic_as_closed
    puts "", "marking topics as closed..."
    
    topics = mysql_query(<<-SQL).to_a
      SELECT first_post_id AS id
            , is_locked AS closed
      FROM #{FLARUM_TABLE_PREFIX}discussions
      WHERE is_locked = true AND hidden_at IS NULL
    SQL

    topics.each do |tinfo|
      parent = topic_lookup_from_imported_post_id(tinfo["id"])
      next unless parent
      topic = Topic.find_by(id: parent[:topic_id])
      next unless topic && topic.first_post

      topic.update_status("closed", true, Discourse.system_user)
    end
  end

  def clean_up(raw)
    return "" if raw.blank?

    guest_user = ensure_guest_user

    # Convert mentions to @username format
    raw.gsub!(/<(POSTMENTION|USERMENTION)[^>]*displayname="([^"]+)"[^>]*>.*?<\/\1>/i) do
      displayname = $2
      user = User.find_by_username(displayname)
      mapped = user&.username
      "@#{mapped || guest_user.username}"
    end

    # Convert <QUOTE> blocks to Markdown blockquotes
    raw.gsub!(%r{<QUOTE>(.*?)</QUOTE>}mi) do
      content = $1.strip

      # Remove paragraph tags and inline tags
      cleaned = content.gsub(%r{<\/?p>}i, '')
                      .gsub(%r{<\/?[^>]+>}i, '')
                      .gsub(/^(&gt;|>)+\s*/i, '') # remove leading fake quote markers

      # Convert each line to blockquote
      quoted = cleaned.lines.map { |line| "> #{line.strip}" }.join("\n")
      "\n#{quoted}\n"
    end

    # Handle <E> tags (emojis or fallback text)
    raw.gsub!(%r{<E>(.*?)</E>}i, '\1')

    # Inline formatting
    raw.gsub!(%r{<i>(.*?)</i>}i, '*\1*')
    raw.gsub!(%r{<b>(.*?)</b>}i, '**\1**')

    # Convert <IMG> tags with alt text to Markdown images on their own lines
    raw.gsub!(%r{<IMG\s+alt="([^"]+)"\s+src="([^"]+)".*?>.*?</IMG>}i) do
      "\n![#{$1}](#{$2})\n"
    end

    # Convert <IMG> tags without alt text to Markdown image placeholder on own lines
    raw.gsub!(%r{<IMG[^>]*src="([^"]+)"[^>]*>.*?</IMG>}i) do
      "\n![image](#{$1})\n"
    end

    # Convert <URL> tags to URLs on their own line for onebox
    raw.gsub!(%r{<URL url="([^"]+)">.*?</URL>}i) do
      "\n#{$1.strip}\n"
    end

    # <<< Added: Properly flatten list items with <p> inside
    raw.gsub!(%r{<LI><s>- </s><p>(.*?)</p></LI>}mi) do
      "- #{$1.strip}"
    end

    # <<< Added: Fallback for <li> without <p>
    raw.gsub!(%r{<LI><s>- </s>(.*?)</LI>}mi) do
      "- #{$1.strip}"
    end

    # <<< Modified: Remove <p> tags *without* turning them into line breaks
    raw.gsub!(%r{<\/?(p)[^>]*>}i, '')

    # Keep line breaks for <br> and <div>
    raw.gsub!(%r{<\/?(br|div)[^>]*>}i, "\n")

    # Remove all other remaining HTML tags
    raw.gsub!(%r{<\/?[^>]+>}, '')

    # Decode HTML entities (e.g. &amp; -> &)
    raw = CGI.unescapeHTML(raw)

    # Normalize line endings
    raw.gsub!(/\r\n?/, "\n")

    # Remove excessive blank lines
    raw.gsub!(/\n{3,}/, "\n\n")

    raw.strip!
    raw
  end

  def mysql_query(sql)
    begin
      @client.query(sql)
    rescue Mysql2::Error => e
      puts "MySQL Query Error: #{e.message}"
    end
  end

end

ImportScripts::Flarum.new.execute
